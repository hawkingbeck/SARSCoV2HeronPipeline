"""
The grapevine variant pipeline outputs nucleotide mutations (SNPs, indels)
and amino acid substitutions (synonymous and nonsynonymous) in separate files.
However, they do not link the nucleotide mutations, or provide
amino acid indels, so we have to do that ourselves here.

Writes out nucleotide to amino acid links for SNPs that can be used to upload to mutation database.
Writes out nucleotide indels, but leaves empty amino acid links for upload to mutation database.
TODO:  translate nucleotide indels to amino acid indels.
"""
import pandas as pd
import numpy as np
import Bio.SeqIO as SeqIO
from Bio.Seq import Seq
import argparse
import csv
from datetime import datetime
import subprocess



def get_aa_at_gene_pos(row, ref_aa_seq_dict):
    """
    Helper function to apply on every row of a data frame
    to find the reference amino acid at given a gene and amino acid position.

    Parameters:
    ==============
    - row: dict
      Row of dataframe in dict form.  Should have fields aa_pos (1-based aa position in gene), gene (gene name)
    - ref_aa_seq_dict: SeqIO dict
      SeqIO Dict containing reference amino acid sequence.
      Should have format {gene_name: SeqRecord of gene amino acid sequence}

    Returns:
    ==============
    - aa: str
      reference amino acid at gene and amino acid position specified in the row
    """
    

    if not row["gene"] or row["gene"] not in ref_aa_seq_dict:
        aa = ""
    else:
        aa_pos_1based = row["aa_pos"]
        aa_pos_0based = aa_pos_1based - 1
        # NB:  the stop codon is never represented in the AA sequences, so they are 1AA shorter than they should be
        if aa_pos_0based == len(ref_aa_seq_dict[row["gene"]].seq):
            aa = "*"
        else:
            aa = ref_aa_seq_dict[row["gene"]].seq[aa_pos_0based]


    return aa


def get_ref_at_nuc_pos(row, ref_nuc_seq_dict):
    """
    Helper function to apply on every row of a data frame
    to find the reference nucleotide at given a genomic nucleotide position.

    Parameters:
    ==============
    - row: dict
      Row of dataframe in dict form.  Should have fields pos (1-based nucleotide position)
    - ref_nuc_seq_dict: SeqIO dict
      SeqIO Dict containing reference genomic sequence.
      Should have format {"MN908947.3": SeqRecord of genome nucleotide sequence}

    Returns:
    ==============
    - nuc: str
      reference nucleotide at genomic nucleotide position specified in the row
    """
    nuc_pos_1based = row["pos"]
    nuc_pos_0based = nuc_pos_1based - 1
    nuc = str(ref_nuc_seq_dict["MN908947.3"].seq[nuc_pos_0based:nuc_pos_0based + 1])
    return nuc


def get_ref_multilen_at_nuc_pos(row, ref_nuc_seq_dict):
    """
    Helper function to apply on every row of a data frame
    to find the reference starting at given a genomic nucleotide position
    and ending at a given length.

    Parameters:
    ==============
    - row: dict
      Row of dataframe in dict form.  Should have fields pos (1-based nucleotide position),
      length (length of reference to extract in bp)
    - ref_nuc_seq_dict: SeqIO dict
      SeqIO Dict containing reference genomic sequence.
      Should have format {"MN908947.3": SeqRecord of genome nucleotide sequence}

    Returns:
    ==============
    - nuc: str
      reference nucleotide at genomic nucleotide position specified in the row
    """
    nuc_pos_1based = row["pos"]
    nuc_pos_0based = nuc_pos_1based - 1
    nuc = str(ref_nuc_seq_dict["MN908947.3"].seq[nuc_pos_0based: nuc_pos_0based + row["length"]])
    return nuc


def convert_nuc_pos_to_aa_pos(gene_df, nuc_mut_df):
    """
    Helper function
    to convert a nucleotide position to an amino acid position within a gene.

    Parameters:
    ==============
    - gene_df: pandas.DataFrame
      dataframe specifying gene coordinates.  Should have columns:
        - start:  nucleotide start position of gene (CDS) coding sequence with respect to genome, 1 based
        - end: nucleotide end position of gene (CDS) coding sequence with respect to genome, 1 based
        - gene:  gene name
        - cds_num:  position of the (CDS) coding sequence within the gene, 0-based.
          A gene can have multiple coding sequences, and they can overlap each other, for
          example if there is programmed ribosomal slippage that causes translation to frameshift backwards/forwards.

    - nuc_mut_df: pandas.DataFrame
      Dataframe specifying nucleotide mutations.  Each row represents a SNP.
      Can have more columns than required columns.
      Required columns:
        - nuc_pos: 1-based nucleotide position of mutation

    Returns:
    ==============
    - nuc_mut_df: str
      Returns a copy of the input nuc_mut_df with new columns:
        - gene: gene name
        - cds_num:  0-based index of coding region within gene
        - aa_pos: 1-based amino acid position of the nucleotide mutation within the gene
        - codon_start_pos:  1-based start position of the codon that covers nuc_pos
        - codon_end_pos:  1-based start position of the codon that covers nuc_pos
    
      Adds a row for each possible amino acid position covered by a nucleotide mutation.
      For example nucleotide mutations that fall in overlapping regions of multiple coding regions or genes
      can map to multiple amino acid positions.

    """

    nuc_mut_in_gene_df_list = []
    for idx, row in gene_df.iterrows():
        nuc_mut_in_range_df = nuc_mut_df.loc[
            ~nuc_mut_df["nuc_pos"].isna() &
            (nuc_mut_df["nuc_pos"] >= row["start"]) &
            (nuc_mut_df["nuc_pos"] <= row["end"])].copy()
        
        nuc_mut_in_range_df["gene"] = row["gene"]
        nuc_mut_in_range_df["cds_num"] = row["cds_num"]

        # Handle discontinuous coding regions within same gene.
        # Handle frameshifts, such as in orf1ab in which
        # programmed ribosomal slippage causes translation to slip 1 base backwards, then continue.
        # AA position is dependent on the total aa length in all previous coding sequences of the gene
        prev_cds_rows = gene_df.loc[
            (gene_df["gene"] == row["gene"]) &
            (gene_df["cds_num"] < row["cds_num"])
        ]
        prev_cds_aa_length = prev_cds_rows["aa_length"].sum()

        # 1-based amino acid position with respect to CDS (coding region), not with respect to entire gene
        aa_pos_wrt_cds_ser = (np.floor((nuc_mut_in_range_df["nuc_pos"] - row["start"]) /3)) + 1
         
        # 1-based amino acid position with respect to entire gene
        nuc_mut_in_range_df["aa_pos"] = aa_pos_wrt_cds_ser + prev_cds_aa_length
        
        # 1-based codon position with respect to entire genome (ie nucleotide coordinates)
        nuc_mut_in_range_df["codon_end_pos"] = aa_pos_wrt_cds_ser * 3 + row["start"]  - 1 
        nuc_mut_in_range_df["codon_start_pos"] = aa_pos_wrt_cds_ser * 3 + row["start"]  - 3                              
                              
        nuc_mut_in_gene_df_list.append(nuc_mut_in_range_df)

    nuc_mut_in_gene_df = pd.concat(nuc_mut_in_gene_df_list)
    # type int won't allow NA values, but type Int64 will
    nuc_mut_in_gene_df["aa_pos"] = nuc_mut_in_gene_df["aa_pos"].astype(float).astype('Int64')
    nuc_mut_in_gene_df["cds_num"] = nuc_mut_in_gene_df["cds_num"].astype(float).astype('Int64')
    nuc_mut_in_gene_df["codon_end_pos"] = nuc_mut_in_gene_df["codon_end_pos"].astype(float).astype('Int64')
    nuc_mut_in_gene_df["codon_start_pos"] = nuc_mut_in_gene_df["codon_start_pos"].astype(float).astype('Int64')

    nuc_mut_out_gene_df = nuc_mut_df[~nuc_mut_df["nuc_pos"].isin(nuc_mut_in_gene_df["nuc_pos"])].copy()
    nuc_mut_out_gene_df["gene"] = ""
    nuc_mut_out_gene_df["cds_num"] = np.nan
    nuc_mut_out_gene_df["aa_pos"] = np.nan
    nuc_mut_out_gene_df["codon_end_pos"] = np.nan
    nuc_mut_out_gene_df["codon_start_pos"] = np.nan
    
    nuc_mut_out_gene_df["aa_pos"] = nuc_mut_out_gene_df["aa_pos"].astype(float).astype('Int64')
    nuc_mut_out_gene_df["cds_num"] = nuc_mut_out_gene_df["cds_num"].astype(float).astype('Int64')
    nuc_mut_out_gene_df["codon_end_pos"] = nuc_mut_out_gene_df["codon_end_pos"].astype(float).astype('Int64')
    nuc_mut_out_gene_df["codon_start_pos"] = nuc_mut_out_gene_df["codon_start_pos"].astype(float).astype('Int64')

    nuc_mut_full_df = pd.concat([nuc_mut_in_gene_df, nuc_mut_out_gene_df])
    nuc_mut_full_df = nuc_mut_full_df.sort_values(["nuc_pos", "gene", "aa_pos"], ascending=True)

    nuc_mut_full_df["aa_pos"] = nuc_mut_full_df["aa_pos"].astype(float).astype('Int64')
    nuc_mut_full_df["cds_num"] = nuc_mut_full_df["cds_num"].astype(float).astype('Int64')
    nuc_mut_full_df["codon_end_pos"] = nuc_mut_full_df["codon_end_pos"].astype(float).astype('Int64')
    nuc_mut_full_df["codon_start_pos"] = nuc_mut_full_df["codon_start_pos"].astype(float).astype('Int64')


    return nuc_mut_full_df


def get_mutated_codon(df, ref_nuc_seq_dict):
    """
    Helper function to get the mutated codon and potentially mutated amino acid.

    Parameters:
    ==============
    - df: pandas.DataFrame
      DataFrame should contain only the rows pertaining to a single codon in a sample,
      as identified by columns: seqHash, gene, cds_num, codon_start_pos, codon_end_pos.
      Each row represents a SNP.
      
      Dataframe can contain more columns than required columns.  
      Required columns:  
      - seqHash
      - gene:  gene name
      - cds_num:  0based index of coding region within gene
      - nuc_pos:  1based genome position of nucleotide mutation
      - nuc_to:  mutated nucleotide at nuc_pos
      - codon_start_pos:  1-based start position of the codon that covers nuc_pos
      - codon_end_pos:  1-based start position of the codon that covers nuc_pos

    - ref_nuc_seq_dict: SeqIO dict
      SeqIO Dict containing reference genomic sequence.
      Should have format {"MN908947.3": SeqRecord of genome nucleotide sequence}

    Returns:
    ==============
    - df: pandas.DataFrame
      Modifies the input df inplace and adds new columns:
      - codon_to: the mutated codon covering position nuc_pos
      - aa_to_translated: the amino acid translation of codon_to
    """
   
    df = df.sort_values(["nuc_pos"], ascending=True)
    codon_to = ""
    for nuc_pos_1based in range(df.iloc[0]["codon_start_pos"], df.iloc[0]["codon_end_pos"]+1):
        
        nuc_pos_0based = nuc_pos_1based - 1
        nuc = str(ref_nuc_seq_dict["MN908947.3"].seq[nuc_pos_0based:nuc_pos_0based + 1])
        if nuc_pos_1based in df["nuc_pos"].values:
            nuc = df.loc[df["nuc_pos"] == nuc_pos_1based, "nuc_to"].values[0] 
        codon_to += nuc
        
    df["codon_to"] = codon_to
    df["aa_to_translated"] = str(Seq(codon_to).translate())

    return df
            
        
        
def convert_nuc_mut_to_aa(nuc_mut_df, ref_nuc_seq_dict):
    """
    Helper function to translate mutated codons into amino acids,
    and only return the SNP - AA associations that yield synonymous substitutions.
    Each row in the input nuc_mut_df represents a SNP and one of its associated amino acid position.
    A SNP can be associated with multiple amino acid positions if it occurs in 
    overlapping genes or overlapping coding regions.
    However, that SNP may or may not yield a synonymous substitution at that that 
    amino acid position.


    Parameters:
    ==============

    - nuc_mut_df: pandas.DataFrame
      Each row represents a SNP and the associated amino acid position.
      Dataframe can contain more columns than required columns.  Required columns:  
      - gene:  name of gene that mutation occurs in, or "" if it doesn't occur in a gene
      - cds_num:  0based index of coding region within the gene
      - nuc_pos:  1based nucleotide position of mutation in the genome
      - nuc_to:  nucleotide mutation at nuc_pos
      - codon_start_pos:  1based genome position of beginning of codon containing the mutation at nuc_pos
      - codon_end_pos:  1based genome position of end of codon containing the mutation at nuc_pos
      
    - ref_nuc_seq_dict: SeqIO dict
      SeqIO Dict containing reference genomic sequence.
      Should have format {"MN908947.3": SeqRecord of genome nucleotide sequence}

    Returns:
    ==============
    - valid_syn_df: pandas.DataFrame
      Makes a copy of the input dataframe and adds new columns:
      - codon_to: the mutated codon covering position nuc_pos
      - aa_to: the amino acid translation of codon_to

      valid_syn_df will only contain the rows in which the nucleotide position corresponds to a 
      synonymous mutation
  
    """


    nuc_mut_trans_df = ( nuc_mut_df
                        .groupby(["seqHash",
                                "gene", "cds_num", "codon_start_pos", "codon_end_pos"])
                        .apply(get_mutated_codon, ref_nuc_seq_dict=ref_nuc_seq_dict)
                      )
    
    valid_syn_df = nuc_mut_trans_df.loc[
        (nuc_mut_trans_df["aa_from"] == nuc_mut_trans_df["aa_to_translated"]) &
        (nuc_mut_trans_df["aa_to_translated"] != "")
    ].reset_index(drop=True)
    valid_syn_df = valid_syn_df.rename(columns={"aa_to_translated": "aa_to"})
    
    return valid_syn_df


def translate_snps(genes_tsv, ref_nuc_fasta_filename, ref_aa_fasta_filename,
                   nuc_mut_tsv, aa_mut_tsv,
                   snp_aa_link_tsv, 
                   gene_overlap_tsv=None):
    """
    Links SNPs to known amino acid substitutions from the output of
    the grapevine variant pipeline.

    The grapevine variant pipeline outputs SNPs and amino acid substitutions (synonymous and nonsynonymous)
    in separate files.  Although it directly converts SNPs to the amino acid substitutions,
    it never writes the linkage down.  So we need to calculate it ourselves.


    Parameters:
    ==============
    - genes_tsv: str
      Path to TSV of gene coordinates.
      Should have columns:
        - start:  nucleotide start position of gene (CDS) coding sequence with respect to genome, 1 based
        - end: nucleotide end position of gene (CDS) coding sequence with respect to genome, 1 based
        - gene:  gene name
        - cds_num:  position of the (CDS) coding sequence within the gene, 0-based.
          A gene can have multiple coding sequences, and they can overlap each other, for
          example if there is programmed ribosomal slippage that causes translation to frameshift backwards/forwards.

    - ref_nuc_fasta_filename: str
      Path to reference nucleotide fasta

    - ref_aa_fasta_filename: str
      Path to reference amino acid fasta

    - nuc_mut_tsv: str
      path to TSV of SNPs.
      Expects that each SNP is on a separate line.
      Columns should be:  seqHash, SNP
      For SNP, format should be "<nuc from><nuc pos><nuc to>"

    - aa_mut_tsv: str
      Path to TSV  of amino acid substitutions.
      Expects that each substitution is on a separate line.
      Columns should be:  seqHash, aa_mutation.
      For aa_mutation:
        - Synonymous substitutions will have format:  synSNP:<nuc from><nuc pos><nuc to>
        - Nonsynonymous substitutions will have format gene:<aa from><aa pos><aa to>

    - snp_aa_link_tsv: str
      Path to output TSV to write nucleotide to amino acid mutation links.
      Will have columns:  ["seqHash",
        "genome_mutation.genome", "genome_mutation.pos", "genome_mutation.ref", "genome_mutation.alt",
        "protein_mutation.gene", "protein_mutation.pos", "protein_mutation.ref", "protein_mutation.alt"]
    
    - gene_overlap_tsv: str
      path to input TSV of coordinates of gene overlap regions.
      Expects columns to be:  start, end, gene_cds
      gene_cds column format should be:  <gene>_cds<0 based cds number within gene>

    Returns:
    ==============
    tuple (link_mut_df, link_mut_ann_df)

    - link_mut_df: pandas.DataFrame
      Dataframe for the nucleotide to amino acid mutation linkage with the columns:
      ["seqHash",
        "genome_mutation.genome", "genome_mutation.pos", "genome_mutation.ref", "genome_mutation.alt",
        "protein_mutation.gene", "protein_mutation.pos", "protein_mutation.ref", "protein_mutation.alt"]

    """
    ref_nuc_seq_dict = SeqIO.to_dict(SeqIO.parse(ref_nuc_fasta_filename, "fasta"))
    ref_aa_seq_dict = SeqIO.to_dict(SeqIO.parse(ref_aa_fasta_filename, "fasta"))

    if gene_overlap_tsv:
        known_overlaps_df = pd.read_csv(gene_overlap_tsv, sep="\t", comment='#')
    else:
        known_overlaps_df = pd.DataFrame(columns=["start", "end", "gene_cds"])

    gene_df = pd.read_csv(genes_tsv, sep="\t", comment="#")
    gene_df["aa_length"] = (gene_df["end"] - gene_df["start"] + 1) / 3

    # Check that distance between end and start is in multiples of 3
    # ie check that start and end correspond to codon start and end
    assert np.sum((gene_df["end"] - gene_df["start"] + 1) % 3  != 0) == 0

    gene_df["aa_length"] = gene_df["aa_length"].astype(int)

    # columns:  seqHash, aa_mutation
    aa_mut_df = pd.read_csv(aa_mut_tsv, sep="\t", comment="#")

    # There might be samples with no amino acid mutations.
    # We drop any samples with empty amino acid mutations to 
    # make merging easier
    aa_mut_df = aa_mut_df.dropna()

    if aa_mut_df.shape[0] < 1:
        nonsyn_mut_df = pd.DataFrame(columns=["gene", "cds_num", "aa_mutation", "aa_from", "aa_pos", "aa_to"])
        syn_mut_df = pd.DataFrame(columns=["nuc_from", "nuc_pos", "nuc_to",
                                            "gene", "cds_num", "aa_mutation", "aa_from", "aa_pos", "aa_to"])
    else:
        # Split up the nonsynonymous and synonymous substitutions from the aa_mut_df,
        # because we need to treat them differently
        nonsyn_mut_df = aa_mut_df[~aa_mut_df["aa_mutation"].str.startswith("synSNP")].copy().reset_index(drop=True)

        nonsyn_mut_df[["gene", "aa_from_pos_to"]] = nonsyn_mut_df["aa_mutation"].str.split(":", expand=True)
        nonsyn_mut_df[["aa_from", "aa_pos", "aa_to"]]  = (nonsyn_mut_df["aa_from_pos_to"]
                                                          .str.extract(r"([A-Z\*])([0-9]+)([A-Z\*]*)", expand=True))
        # type int won't allow NA values but type Int64 will.
        # But oddly, we need to cast to float before we can cast to int64
        nonsyn_mut_df["aa_pos"] = nonsyn_mut_df["aa_pos"].astype('float').astype('Int64')

        syn_mut_df = aa_mut_df[aa_mut_df["aa_mutation"].str.startswith("synSNP")].copy().reset_index(drop=True)
        syn_mut_df[["consequence", "nuc_from_pos_to"]] = syn_mut_df["aa_mutation"].str.split(":", expand=True)
        syn_mut_df[["nuc_from", "nuc_pos", "nuc_to"]]  = syn_mut_df["nuc_from_pos_to"].str.extract(r"([A-Z])([0-9]+)([A-Z])", expand=True)
        syn_mut_df["nuc_pos"] = syn_mut_df["nuc_pos"].astype(float).astype("Int64")

        # Has columns:  seqHash, aa_mutation, nuc_from, nuc_pos, nuc_to
        # Also has throwaway columns:  consequence, nuc_from_pos_to.
        # Append columns:  gene, cds_num, aa_pos, codon_start_pos, codon_end_pos
        # Each row represents a SNP that we know should lead to a synonymous substitution (according to gofasta)
        # If a SNP happens to cover multiple amino acid positions because it hits an overlapping gene region, overlapping coding region,
        # we add another row to represent each SNP - amino acid position mapping.
        syn_mut_df = convert_nuc_pos_to_aa_pos(gene_df=gene_df, nuc_mut_df=syn_mut_df)
        syn_mut_df["aa_pos"] = syn_mut_df["aa_pos"].astype('float').astype('Int64')

        # https://stackoverflow.com/questions/43196907/valueerror-wrong-number-of-items-passed-meaning-and-suggestions
        # apply on empty dataframe borks:
        if syn_mut_df.shape[0] > 0:
            syn_mut_df["aa_from"] = syn_mut_df.apply(get_aa_at_gene_pos, axis="columns", ref_aa_seq_dict=ref_aa_seq_dict)
        else:
            syn_mut_df["aa_from"] = ""

        # Has columns:  seqHash, aa_mutation, nuc_from, nuc_pos, nuc_to, gene, cds_num, aa_pos, codon_start_pos, codon_end_pos
        # Also has throwaway columns:  consequence, nuc_from_pos_to.
        # Append columns:  codon_to, aa_to.  codon_to is a throwaway column we won't use later.
        # Cull the rows such that only SNP - amino acid position mappings 
        # that result in synonymous substitutions exist.
        syn_mut_df = convert_nuc_mut_to_aa(ref_nuc_seq_dict=ref_nuc_seq_dict,  nuc_mut_df=syn_mut_df)

    

    # Columns: seqHash, SNP
    nuc_mut_df = pd.read_csv(nuc_mut_tsv, sep="\t", comment="#")
    
    # There might be samples with no SNPs
    # We drop those samples to make merging easier
    nuc_mut_df = nuc_mut_df.dropna()

    
    if nuc_mut_df.shape[0] > 0:
        nuc_mut_df[["nuc_from", "nuc_pos", "nuc_to"]] = nuc_mut_df["SNP"].str.extract(r"([A-Z]*)([0-9]+)([A-Z]*)", expand=True)
        nuc_mut_df["nuc_pos"] = nuc_mut_df["nuc_pos"].astype(float).astype("Int64")
        
        # Has Columns: seqHash, SNP, nuc_from, nuc_to, nuc_pos.  
        # Append columns: gene, cds_num, aa_pos, codon_start_pos, codon_end_pos
        # We want each row to represent a SNP - amino acid position mapping.
        # If a SNP happens to cover multiple amino acid positions because it hits an overlapping gene region, overlapping coding region,
        # the SNP will be repeated in multiple rows, one for each amino acid position mapping.
        nuc_mut_df = convert_nuc_pos_to_aa_pos(gene_df=gene_df, nuc_mut_df=nuc_mut_df)

        # Has Columns: seqHash, SNP, nuc_from, nuc_to, nuc_pos, gene, cds_num, aa_pos, codon_start_pos, codon_end_pos
        # Append: aa_from
        nuc_mut_df["aa_from"] = nuc_mut_df.apply(get_aa_at_gene_pos, axis="columns", ref_aa_seq_dict=ref_aa_seq_dict)

    # Now link nucleotide mutations with amino acid substitutions

    if nuc_mut_df.shape[0] == 0 and (nonsyn_mut_df.shape[0] > 0 or syn_mut_df.shape[0] > 0):
        raise ValueError("Invalid:  We have nonsynonymous or synonymous AA substitutions, but no SNPs.")

    # Handle situtations in which samples have no mutations at all
    if nuc_mut_df.shape[0] == 0 and nonsyn_mut_df.shape[0] == 0 and syn_mut_df.shape[0] == 0:
        link_mut_df = pd.DataFrame(columns=[
            "seqHash",
            "nuc_from", "nuc_pos", "nuc_to", "SNP",
            "aa_from", "aa_pos", "aa_to",
            "gene", "cds_num", "aa_mutation"])

    # Handle situations where samples only have SNPs in non-gene regions
    elif nuc_mut_df.shape[0] >  0 and (nonsyn_mut_df.shape[0] == 0 and syn_mut_df.shape[0] == 0):
        link_mut_df = nuc_mut_df.copy()
        link_mut_df["aa_to"] = ""

    # Handle situations in which samples have SNPs
    # as well as nonsynonymous or synonymous substitutions
    else:
        #  Multiple SNPs might contribute to a single nonsyn amino acid substitution.
        # So there may be nonsyn_mut_df rows that get duplicated against multiple nuc_mut_df rows.

        # A single SNP position may map to multiple AA positions if they are in overlapping genes or coding regions.

        
        if nonsyn_mut_df.shape[0] == 0:
            link_nonsyn_mut_df = pd.DataFrame(
                columns=["seqHash", "gene", "nuc_from", "nuc_pos", "nuc_to", "SNP",
                         "aa_from", "aa_pos", "aa_to", "aa_mutation"])
        else:
            # Columns after merging (but not necessarily in that order):
            # seqHash, SNP, nuc_from, nuc_to, nuc_pos,
            # gene, cds_num, codon_start_pos, codon_end_pos, 
            # aa_pos, aa_from, aa_to, aa_mutation,
            # nuc_from_pos_to, aa_from_pos_to
            link_nonsyn_mut_df = nuc_mut_df.merge(nonsyn_mut_df, how="right",
                                      left_on=["seqHash", "gene", "aa_from", "aa_pos"],
                                      right_on=["seqHash", "gene", "aa_from", "aa_pos"])

    
            # are there nonsyn mutations that don't have a corresponding SNP?
            if np.sum(~link_nonsyn_mut_df["aa_mutation"].isna() & link_nonsyn_mut_df["SNP"].isna()) > 0:
                print(link_nonsyn_mut_df)
                print(link_nonsyn_mut_df[link_nonsyn_mut_df["SNP"].isna()])
                raise ValueError("There are nonynonymous AA substitutions that don't have a corresponding SNP")
      

        if syn_mut_df.shape[0] == 0:
            link_syn_mut_df = pd.DataFrame(columns=[
              "seqHash", 
              "SNP", "nuc_from", "nuc_pos", "nuc_to",
              "aa_mutation", "gene", "aa_from", "aa_pos", "aa_to"])
        else:
            # Columns after merging (but not necessarily in that order):
            # seqHash, SNP, nuc_from, nuc_to, nuc_pos,
            # gene, cds_num, codon_start_pos, codon_end_pos, aa_pos, aa_from, consequence
            link_syn_mut_df = nuc_mut_df.merge(syn_mut_df, how="right",
                              left_on=["seqHash", "gene", "cds_num", "nuc_from", "nuc_pos", "nuc_to",
                                       "aa_from", "aa_pos"],
                              right_on=["seqHash", "gene", "cds_num", "nuc_from", "nuc_pos", "nuc_to",
                                       "aa_from", "aa_pos"])

            # are there syn mutations that don't have a corresponding SNP?
            if np.sum(~link_syn_mut_df["aa_mutation"].isna() & link_syn_mut_df["SNP"].isna()) > 0:
                print(link_syn_mut_df[link_syn_mut_df["SNP"].isna()])
                raise ValueError("There are synonymous substitutions that don't have a corresponding SNP")


        link_nonsyn_syn_mut_df = pd.concat([link_nonsyn_mut_df, link_syn_mut_df])

        # merge in SNPs that aren't in any genes so can't be associated with AA substitution
        # or SNPs that should be associated with AA substitutions but gofasta fails to report them
        # See https://gitlab.internal.sanger.ac.uk/heron/core-data-flow/-/issues/112
        uniq_snp_df = nuc_mut_df[["seqHash", "SNP", "nuc_from", "nuc_to", "nuc_pos"]]
        uniq_snp_df = uniq_snp_df[~uniq_snp_df.duplicated()]
        link_mut_df = (uniq_snp_df.merge(link_nonsyn_syn_mut_df,
                   how="outer",
                   left_on=["seqHash", "SNP", "nuc_from", "nuc_to", "nuc_pos"],
                   right_on=["seqHash", "SNP", "nuc_from", "nuc_to", "nuc_pos"]) 
        )
        link_mut_df["aa_mutation"] = link_mut_df["aa_mutation"].fillna("") # pandas v1.0.* chokes on .duplicated() if NA
        link_mut_df = link_mut_df[~link_mut_df.duplicated(
          ["seqHash", "gene", "SNP", "aa_mutation"])].copy()
        link_mut_df = link_mut_df.sort_values(["seqHash", "nuc_pos", "aa_pos"], ascending=True).reset_index(drop=True)

        
        # Check if a SNP is associated with multiple AA mutations (nonsynoymous or synonymous).  
        # This should be impossible except at positions
        # in overlapping genes, such as 13468bp, which is at the end nucleotide of gene orf1a 
        # and beginning nucleotide of gene orf1b.
        # All the overlapping regions should be passed in as TSV separate from the genes.tsv.


        aa_mut_count_by_snp_df = (link_mut_df
            .groupby(["seqHash", "SNP", "nuc_from", "nuc_pos", "nuc_to"])
            .size()
            .to_frame()
        )
        aa_mut_count_by_snp_df.columns = ["aa_mut_count"]
        # Columns should be seqHash, SNP, nuc_from, nuc_pos, nuc_to, aa_mut_count
        aa_mut_count_by_snp_df = aa_mut_count_by_snp_df.reset_index(drop=False)

        if np.sum(aa_mut_count_by_snp_df["aa_mut_count"] > 1) > 0:
            fishy_df = aa_mut_count_by_snp_df[aa_mut_count_by_snp_df["aa_mut_count"] > 1].copy()

            fishy_df["in_overlap"] = False
            for idx, known_overlap in known_overlaps_df.iterrows():
                fishy_df["in_overlap"] = (((fishy_df["nuc_pos"] >= known_overlap["start"]) & 
                                            (fishy_df["nuc_pos"] <= known_overlap["end"])
                                          ) |
                                          fishy_df["in_overlap"]
                )

            if not np.all(fishy_df["in_overlap"]):
                print(link_mut_df)
                print(fishy_df)
                raise ValueError("There are SNPs that are associated with multiple AA sustitutions.  " + 
                  "And these SNPs are not in overlapping genes.  This should not happen")


    
    
    # Write out database friendly column names of mutation linkage to TSV
    link_mut_out_df = link_mut_df.rename(columns={
        "seqHash": "seqHash",
        "nuc_from": "genome_mutation.ref",
        "nuc_to": "genome_mutation.alt",
        "nuc_pos": "genome_mutation.pos",
        "aa_from": "protein_mutation.ref",
        "aa_to": "protein_mutation.alt",
        "aa_pos": "protein_mutation.pos",
        "gene": "protein_mutation.gene",
    })


    link_mut_out_df["genome_mutation.genome"] = "MN908947.3"
    link_mut_out_df = link_mut_out_df[
        ["seqHash",
        "genome_mutation.genome", "genome_mutation.pos", "genome_mutation.ref", "genome_mutation.alt",
        "protein_mutation.gene", "protein_mutation.pos", "protein_mutation.ref", "protein_mutation.alt"]
        ]

    link_mut_out_df.to_csv(snp_aa_link_tsv, sep="\t", index=False, header=True)
    
    # Write out database friendly column names of mutation linkage to TSV
    link_mut_out_df = link_mut_df.rename(columns={
        "seqHash": "seqHash",
        "nuc_from": "genome_mutation.ref",
        "nuc_to": "genome_mutation.alt",
        "nuc_pos": "genome_mutation.pos",
        "aa_from": "protein_mutation.ref",
        "aa_to": "protein_mutation.alt",
        "aa_pos": "protein_mutation.pos",
        "gene": "protein_mutation.gene",
    })


    link_mut_out_df["genome_mutation.genome"] = "MN908947.3"
    link_mut_out_df = link_mut_out_df[
        ["seqHash",
        "genome_mutation.genome", "genome_mutation.pos", "genome_mutation.ref", "genome_mutation.alt",
        "protein_mutation.gene", "protein_mutation.pos", "protein_mutation.ref", "protein_mutation.alt"]
        ]

    link_mut_out_df.to_csv(snp_aa_link_tsv, sep="\t", index=False, header=True)

    return link_mut_out_df


def translate_deletions(genes_tsv, ref_nuc_fasta_filename, ref_aa_fasta_filename,
                        nuc_del_tsv, del_nuc_aa_link_tsv):
    """
    Links nucleotide deletions from the output of
    the grapevine variant pipeline to amino acid positions.

    The grapevine variant pipeline only outputs nucleotide deletions, but not amino acid translations,
    so we need to do the conversion to amino acid ourselves.

    TODO:  actually implement the conversion of nucleotide to amino acid


    Parameters:
    ==============
    - genes_tsv: str
      Path to TSV of gene coordinates.
      Should have columns:
        - start:  nucleotide start position of gene (CDS) coding sequence with respect to genome, 1 based
        - end: nucleotide end position of gene (CDS) coding sequence with respect to genome, 1 based
        - gene:  gene name
        - cds_num:  position of the (CDS) coding sequence within the gene, 0-based.
          A gene can have multiple coding sequences, and they can overlap each other, for
          example if there is programmed ribosomal slippage that causes translation to frameshift backwards/forwards.

    - ref_nuc_fasta_filename: str
      Path to reference nucleotide fasta

    - ref_aa_fasta_filename: str
      Path to reference amino acid fasta

    - nuc_del_tsv: str
      path to TSV of nucleotide deletions.
      Expects that each deletion is on a separate line.
      Columns should be:  seqHash, ref_start, length
        - ref_start is the 1-based start position of the deletion with resect to reference genome.
        - length is the length of the deletion with respect to the reference genome in bp

    - del_nuc_aa_link_tsv: str
      Path to output TSV to write nucleotide to amino acid mutation deletion link.
      Will have columns:  ["seqHash",
        "genome_mutation.genome", "genome_mutation.pos", "genome_mutation.ref", "genome_mutation.alt",
        "protein_mutation.gene", "protein_mutation.pos", "protein_mutation.ref", "protein_mutation.alt"]
        - genome_mutation.ref:  reference sequence through the deletion.  Can be multiple bp long.
        - genome_mutation.alt: format "del<length>", where length
          is the length of the deletion in bp.
        - genome_mutation.pos:  1-based position of the start of the deletion with respect to reference genome
        - protein.*:  all these columns are not implemented and will be blank.  TODO:  implement them


    Returns:
    ==============
    - nuc_del_out_df: pandas.DataFrame
      Dataframe for the nucleotide to amino acid mutation linkage with the columns:
      ["seqHash",
        "genome_mutation.genome", "genome_mutation.pos", "genome_mutation.ref", "genome_mutation.alt",
        "protein_mutation.gene", "protein_mutation.pos", "protein_mutation.ref", "protein_mutation.alt"]

        - genome_mutation.ref:  reference sequence through the deletion.  Can be multiple bp long.
        - genome_mutation.alt: format "del<length>", where length
          is the length of the deletion in bp.
        - genome_mutation.pos:  1-based position of the start of the deletion with respect to reference genome
        - protein.*:  all these columns are not implemented and will be blank.  TODO:  implement them

    """
    ref_nuc_seq_dict = SeqIO.to_dict(SeqIO.parse(ref_nuc_fasta_filename, "fasta"))
    ref_aa_seq_dict = SeqIO.to_dict(SeqIO.parse(ref_aa_fasta_filename, "fasta"))


    gene_df = pd.read_csv(genes_tsv, sep="\t", comment="#")
    gene_df["aa_length"] = (gene_df["end"] - gene_df["start"] + 1) / 3

    # Check that distance between end and start is in multiples of 3
    # ie check that start and end correspond to codon start and end
    assert np.sum((gene_df["end"] - gene_df["start"] + 1) % 3  != 0) == 0

    gene_df["aa_length"] = gene_df["aa_length"].astype(int)

    nuc_del_df = pd.read_csv(nuc_del_tsv, sep="\t")
    # There might be samples with no mutations
    # We drop any samples with null mutations before uploading them into the database.
    nuc_del_df = nuc_del_df.dropna()
    nuc_del_df["pos"] = nuc_del_df["ref_start"].astype(int)
    nuc_del_df["length"] = nuc_del_df["length"].astype(int)


    # https://stackoverflow.com/questions/43196907/valueerror-wrong-number-of-items-passed-meaning-and-suggestions
    # apply on empty dataframe borks:
    # File "/opt/miniconda3/envs/grapevine/lib/python3.7/site-packages/pandas/core/internals/blocks.py", line 143, in __init__
    #  f"Wrong number of items passed {len(self.values)}, "
    if nuc_del_df.shape[0] > 0:
        nuc_del_df["ref"] = nuc_del_df.apply(get_ref_multilen_at_nuc_pos, axis="columns", ref_nuc_seq_dict=ref_nuc_seq_dict)
    else:
        nuc_del_df["ref"] = ""
    nuc_del_df["alt"] = "del" + nuc_del_df["length"].astype(str)

    nuc_del_out_df = nuc_del_df.rename(columns={
        "seqHash": "seqHash",
        "ref": "genome_mutation.ref",
        "alt": "genome_mutation.alt",
        "pos": "genome_mutation.pos"
    })

    nuc_del_out_df["genome_mutation.genome"] = "MN908947.3"
    nuc_del_out_df["protein_mutation.ref"] = ""
    nuc_del_out_df["protein_mutation.alt"] = ""
    nuc_del_out_df["protein_mutation.pos"] = ""
    nuc_del_out_df["protein_mutation.gene"] = ""

    nuc_del_out_df = nuc_del_out_df[["seqHash",
        "genome_mutation.genome", "genome_mutation.pos", "genome_mutation.ref", "genome_mutation.alt",
        "protein_mutation.gene", "protein_mutation.pos", "protein_mutation.ref", "protein_mutation.alt"]
        ]

    nuc_del_out_df.to_csv(del_nuc_aa_link_tsv, sep="\t", index=False, header=True)

    return nuc_del_out_df


def translate_insertions(genes_tsv, ref_nuc_fasta_filename, ref_aa_fasta_filename,
                        nuc_ins_tsv, ins_nuc_aa_link_tsv):
    """
    Links nucleotide insertions from the output of
    the grapevine variant pipeline to amino acid positions.

    The grapevine variant pipeline only outputs nucleotide insertions but not amino acid translations,
    so we need to do the conversion to amino acid ourselves.

    TODO:  actually implement the conversion of nucleotide to amino acid


    Parameters:
    ==============
    - genes_tsv: str
      Path to TSV of gene coordinates.
      Should have columns:
        - start:  nucleotide start position of gene (CDS) coding sequence with respect to genome, 1 based
        - end: nucleotide end position of gene (CDS) coding sequence with respect to genome, 1 based
        - gene:  gene name
        - cds_num:  position of the (CDS) coding sequence within the gene, 0-based.
          A gene can have multiple coding sequences, and they can overlap each other, for
          example if there is programmed ribosomal slippage that causes translation to frameshift backwards/forwards.

    - ref_nuc_fasta_filename: str
      Path to reference nucleotide fasta

    - ref_aa_fasta_filename: str
      Path to reference amino acid fasta

    - nuc_ins_tsv: str
      path to TSV of nucleotide insertions.
      Expects that each deletion is on a separate line.
      Columns should be:  seqHash, ref_start, insertion
        - ref_start is the 1-based start position right before the insertion with resect to reference genome.
        - insertion is the inserted nucleotide sequence

    - ins_nuc_aa_link_tsv: str
      Path to output TSV to write nucleotide to amino acid mutation insertions link.
      Will have columns:  ["seqHash",
        "genome_mutation.genome", "genome_mutation.pos", "genome_mutation.ref", "genome_mutation.alt",
        "protein_mutation.gene", "protein_mutation.pos", "protein_mutation.ref", "protein_mutation.alt"]
        - genome_mutation.ref:  reference base right before the insertion
        - genome_mutation.alt: format "insert<insertion sequence>", where insertion sequence is
          just the inserted sequence and does not include any reference bases.
        - genome_mutation.pos:  1-based position of with respect to the reference genome right before the insertion
        - protein.*:  all these columns are not implemented and will be blank.  TODO:  implement them

    Returns:
    ==============
    - nuc_ins_out_df: pandas.DataFrame
      Dataframe for the nucleotide to amino acid mutation linkage with the columns:
      ["seqHash",
        "genome_mutation.genome", "genome_mutation.pos", "genome_mutation.ref", "genome_mutation.alt",
        "protein_mutation.gene", "protein_mutation.pos", "protein_mutation.ref", "protein_mutation.alt"]

        - genome_mutation.ref:  reference base right before the insertion
        - genome_mutation.alt: format "insert<insertion sequence>", where insertion sequence is
          just the inserted sequence and does not include any reference bases.
        - genome_mutation.pos:  1-based position of with respect to the reference genome right before the insertion
        - protein.*:  all these columns are not implemented and will be blank.  TODO:  implement them

    """
    ref_nuc_seq_dict = SeqIO.to_dict(SeqIO.parse(ref_nuc_fasta_filename, "fasta"))
    ref_aa_seq_dict = SeqIO.to_dict(SeqIO.parse(ref_aa_fasta_filename, "fasta"))


    gene_df = pd.read_csv(genes_tsv, sep="\t", comment="#")
    gene_df["aa_length"] = (gene_df["end"] - gene_df["start"] + 1) / 3

    # Check that distance between end and start is in multiples of 3
    # ie check that start and end correspond to codon start and end
    assert np.sum((gene_df["end"] - gene_df["start"] + 1) % 3  != 0) == 0

    gene_df["aa_length"] = gene_df["aa_length"].astype(int)


    nuc_ins_df = pd.read_csv(nuc_ins_tsv, sep="\t", comment="#")
    # There might be samples with no mutations
    # We drop any samples with null mutations before uploading them into the database.
    nuc_ins_df = nuc_ins_df.dropna()
    nuc_ins_df["pos"] = nuc_ins_df["ref_start"].astype(int)

    # https://stackoverflow.com/questions/43196907/valueerror-wrong-number-of-items-passed-meaning-and-suggestions
    # apply on empty dataframe borks:
    # File "/opt/miniconda3/envs/grapevine/lib/python3.7/site-packages/pandas/core/internals/blocks.py", line 143, in __init__
    #  f"Wrong number of items passed {len(self.values)}, "
    if nuc_ins_df.shape[0] > 0:
        nuc_ins_df["ref"] = nuc_ins_df.apply(get_ref_at_nuc_pos, axis="columns", ref_nuc_seq_dict=ref_nuc_seq_dict)
    else:
        nuc_ins_df["ref"] = ""
    nuc_ins_df["alt"] = "insert" + nuc_ins_df["insertion"]

    nuc_ins_out_df = nuc_ins_df.rename(columns={
        "seqHash": "seqHash",
        "ref": "genome_mutation.ref",
        "alt": "genome_mutation.alt",
        "pos": "genome_mutation.pos"
    })

    nuc_ins_out_df["genome_mutation.genome"] = "MN908947.3"
    nuc_ins_out_df["protein_mutation.ref"] = ""
    nuc_ins_out_df["protein_mutation.alt"] = ""
    nuc_ins_out_df["protein_mutation.pos"] = ""
    nuc_ins_out_df["protein_mutation.gene"] = ""

    nuc_ins_out_df = nuc_ins_out_df[["seqHash",
        "genome_mutation.genome", "genome_mutation.pos", "genome_mutation.ref", "genome_mutation.alt",
        "protein_mutation.gene", "protein_mutation.pos", "protein_mutation.ref", "protein_mutation.alt"]
        ]

    nuc_ins_out_df.to_csv(ins_nuc_aa_link_tsv, sep="\t", index=False, header=True)

    return nuc_ins_out_df


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='Links SNPs and AA substitutions output by the grapevine pipeline.')

    parser.add_argument('--ref_nuc_fasta_filename', type=str,
                        help='Path to reference nucleotide fasta.  Required.')
    parser.add_argument('--ref_aa_fasta_filename', type=str,
                        help='Path to reference amino acid fasta.  Required.')
    parser.add_argument('--genes_tsv', type=str,
                        help='Path to TSV of gene coordinates.  Required.'  +
                          'Should have columns: start, end, gene, cds_num')
    parser.add_argument('--gene_overlap_tsv', type=str,
                        help='Path to TSV of gene overlap coordinates.  Optional if no genes overlap.'  +
                          'Should have columns: start, end, gene_cds')
    parser.add_argument('--nuc_ins_tsv', type=str,
                        help="path to TSV of nucleotide insertions.  Required.  " +
                          "Columns should be:  seqHash, ref_start, insertion")
    parser.add_argument('--nuc_mut_tsv', type=str,
                        help="Path to TSV of SNPs.  Required.  " +
                          "Columns should be:  seqHash, SNP")
    parser.add_argument('--nuc_del_tsv', type=str,
                        help="Path to TSV of nucleotide deletions.  Required.  " +
                          "Columns should be:  seqHash, ref_start, length")
    parser.add_argument('--aa_mut_tsv', type=str,
                        help="Path to TSV  of amino acid substitutions.  Required.  " +
                          "Columns should be:  seqHash, aa_mutation")
    parser.add_argument('--snp_aa_link_tsv', type=str,
                        help="Path to output TSV to write nucleotide to amino acid mutation links. Required.  " +
                          "Columns:  seqHash, " +
                          "genome_mutation.genome, genome_mutation.pos, genome_mutation.ref, genome_mutation.alt, " +
                          "protein_mutation.gene, protein_mutation.pos, protein_mutation.ref, protein_mutation.alt")
    parser.add_argument('--ins_nuc_aa_link_tsv', type=str,
                        help="Path to output TSV to write nucleotide to amino acid mutation insertion link. Required.  " +
                          "Columns:  seqHash, " +
                          "genome_mutation.genome, genome_mutation.pos, genome_mutation.ref, genome_mutation.alt, " +
                          "protein_mutation.gene, protein_mutation.pos, protein_mutation.ref, protein_mutation.alt")
    parser.add_argument('--del_nuc_aa_link_tsv', type=str,
                        help="Path to output TSV to write nucleotide to amino acid mutation deletion link. Required.  " +
                          "Columns:  seqHash, " +
                          "genome_mutation.genome, genome_mutation.pos, genome_mutation.ref, genome_mutation.alt, " +
                          "protein_mutation.gene, protein_mutation.pos, protein_mutation.ref, protein_mutation.alt")

    args = parser.parse_args()
    ref_nuc_fasta_filename = args.ref_nuc_fasta_filename
    ref_aa_fasta_filename = args.ref_aa_fasta_filename

    genes_tsv = args.genes_tsv
    gene_overlap_tsv = args.gene_overlap_tsv
    aa_mut_tsv = args.aa_mut_tsv
    nuc_mut_tsv = args.nuc_mut_tsv
    nuc_ins_tsv = args.nuc_ins_tsv
    nuc_del_tsv = args.nuc_del_tsv
    snp_aa_link_tsv = args.snp_aa_link_tsv
    ins_nuc_aa_link_tsv = args.ins_nuc_aa_link_tsv
    del_nuc_aa_link_tsv = args.del_nuc_aa_link_tsv



    link_mut_out_df, link_mut_ann_df = translate_snps(
                    genes_tsv=genes_tsv, ref_nuc_fasta_filename=ref_nuc_fasta_filename,
                    ref_aa_fasta_filename=ref_aa_fasta_filename,
                    nuc_mut_tsv=nuc_mut_tsv, aa_mut_tsv=aa_mut_tsv,
                    snp_aa_link_tsv=snp_aa_link_tsv, 
                    gene_overlap_tsv=gene_overlap_tsv)


    nuc_del_out_df = translate_deletions(genes_tsv=genes_tsv,
                                         ref_nuc_fasta_filename=ref_nuc_fasta_filename,
                                         ref_aa_fasta_filename=ref_aa_fasta_filename,
                                         nuc_del_tsv=nuc_del_tsv,
                                         del_nuc_aa_link_tsv=del_nuc_aa_link_tsv)


    nuc_ins_out_df = translate_insertions(genes_tsv=genes_tsv,
                                          ref_nuc_fasta_filename=ref_nuc_fasta_filename,
                                          ref_aa_fasta_filename=ref_aa_fasta_filename,
                                          nuc_ins_tsv=nuc_ins_tsv,
                                          ins_nuc_aa_link_tsv=ins_nuc_aa_link_tsv)