#!/usr/bin/env python
"""
Calls mutations.  Usually just a wrapper for govariants.
"""

import os
import subprocess
import sys
import shutil
import pandas as pd
import argparse
import numpy as np

# Columns to filter metadata dataframe to before merging
MODE_AA_MUT = "aa_mutations"
MODE_NUC_MUT = "nuc_mutations"
MODE_NUC_INDEL = "nuc_indels"


def call_aa_mutations(seqHash, output_tsv, sam, reference_fasta, reference_genbank, threads):

    # From https://github.com/cov-ert/gofasta/blob/master/cmd/variants.go:
    # The output is a csv-format file with one line per query sequence, and two columns: 'query' and
    # 'variants', the second of which is a "|"-delimited list of amino acid changes and synonymous SNPs
    # in that query relative to the reference sequence specified using --reference/-r.
    # EG)  synSNP:C913T|synSNP:C3037T|orf1ab:T1001I|orf1ab:A1708D|synSNP:C5986T|orf1ab:I2230T
    #
    # But what we want is each mutation on a separate line.
    # For now we only output aa substitutions (ie nonsynonymous substitutions)
    cmd = ["gofasta", "sam", "variants",
            "-t", str(threads),
            "--samfile", sam,
            "--reference", reference_fasta,
            "--genbank", reference_genbank,
            "--outfile", "gofasta_sam_variants.out.csv"]

    proc = subprocess.run(cmd, check=True)

  
    raw_aa_mut_df = pd.read_csv('gofasta_sam_variants.out.csv', sep=",",
                                keep_default_na=False, na_values=[], dtype=str)

    # If there are no variants, gofasta v0.03 will still output a line with query and empty variants field.
    # EG)
    # query,variants
    # Consensus_39402_2#89.primertrimmed.consensus_threshold_0.75_quality_20,
    #
    # We only want to output a row if the sample actually has amino acid variants
    raw_aa_mut_df = raw_aa_mut_df.dropna()

    if raw_aa_mut_df.shape[0] > 0:
        # aa_mut_df = pd.concat([meta_df, raw_aa_mut_df[["variants"]]], axis=1)
        aa_mut_df = raw_aa_mut_df[["variants"]].copy()
        aa_mut_df['seqHash'] = seqHash
        split_mut_ser = aa_mut_df["variants"].str.split("|", expand=True).stack()
        split_mut_ser = split_mut_ser.reset_index(drop=True, level=1)  # to line up with df's index
        split_mut_ser.name = "aa_mutation"

        aa_mut_df = aa_mut_df.join(split_mut_ser).reset_index(drop=True)
        aa_mut_df = aa_mut_df.drop(columns=["variants"]).reset_index(drop=True)

    else:
        aa_mut_df = pd.DataFrame(columns=["seqHash"] + ["aa_mutation"])

    
    aa_mut_df.to_csv(output_tsv, sep="\t", header=True, index=False)

def call_nuc_indels(seqHash, sam, output_prefix):

    # From https://github.com/cov-ert/gofasta/blob/master/cmd/indels.go,
    # gofasta sam indels outputs a TSV for insertions and TSV for deletions.
    # One line for each insertion/deletion position.
    # Insertion columns:  ref_start, insertion, samples
    # Deletions columns:  ref_start, length, samples
    # --threshold is the minimum length of indel to be included in output
    cmd = ["gofasta", "sam", "indels",
              "-s", sam,
              "--threshold", "1",
              "--insertions-out", "gofasta_sam_indels.out.insertions.tsv",
              "--deletions-out", "gofasta_sam_indels.out.deletions.tsv"]

    proc = subprocess.run(cmd, check=True)

    # meta_df = pd.read_csv(metadata_tsv, sep="\t",
    #                       keep_default_na=False, na_values=[], dtype=str)

    raw_nuc_insert_df = pd.read_csv('gofasta_sam_indels.out.insertions.tsv', sep="\t",
                                    keep_default_na=False, na_values=[], dtype=str)

    # Drop any rows with empty mutations, just in case
    raw_nuc_insert_df = raw_nuc_insert_df.dropna()

    insert_tsv = output_prefix + ".insertions.tsv"
    # https://stackoverflow.com/questions/13269890/cartesian-product-in-pandas
    # workaround for cartesian product in pandas < v1.2

    if raw_nuc_insert_df.shape[0] > 0:
        nuc_insert_df = raw_nuc_insert_df[["ref_start", "insertion"]]
        # nuc_insert_df = (meta_df.assign(key=1)
        #                         .merge(
        #                             raw_nuc_insert_df[["ref_start", "insertion"]].assign(key=1),
        #                             how="outer", on="key")
        #                         .drop("key", axis=1))
        nuc_insert_df['seqHash'] = seqHash
    else:
        nuc_insert_df = pd.DataFrame(columns=["seqHash"] + ["ref_start", "insertion"])

    nuc_insert_df.to_csv(insert_tsv, sep="\t", header=True, index=False)

    raw_nuc_del_df = pd.read_csv('gofasta_sam_indels.out.deletions.tsv', sep="\t",
                                    keep_default_na=False, na_values=[], dtype=str)

    # Drop any rows with empty deletions, just in case
    raw_nuc_del_df = raw_nuc_del_df.dropna()

    del_tsv = output_prefix + ".deletions.tsv"

    if raw_nuc_del_df.shape[0] > 0:
        nuc_del_df = raw_nuc_del_df[["ref_start", "length"]]
        # nuc_del_df = (meta_df.assign(key=1)
        #                         .merge(
        #                             raw_nuc_del_df[["ref_start", "length"]].assign(key=1),
        #                             how="outer", on="key")
        #                         .drop("key", axis=1))
        nuc_del_df['seqHash'] = seqHash
    else:
        # nuc_del_df = pd.DataFrame(columns=meta_df.columns.tolist() + ["ref_start", "length"])
        nuc_del_df = pd.DataFrame(columns=["seqHash"] + ["ref_start", "length"])

    nuc_del_df.to_csv(del_tsv, sep="\t", header=True, index=False)

def call_nuc_mutations(seqHash, reference_fasta, aligned_fasta, output_tsv):

    # https://github.com/cov-ert/gofasta/blob/master/cmd/snps.go
    # The output is a csv-format file with one line per query sequence, and two columns:
    # 'query' and 'SNPs', the second of which is a "|"-delimited list of snps in that query
    cmd = ["gofasta", "snps",
           "-r", reference_fasta,
           "-q", aligned_fasta,
           "-o", "gofasta.snps.csv"]

    proc = subprocess.run(cmd, check=True)

    raw_nuc_mut_df = pd.read_csv('gofasta.snps.csv', sep=",",
                                keep_default_na=False, na_values=[], dtype=str)

    # If there are no SNPs, gofasta v0.03 will still output a line with query and empty SNPs field.
    # EG)
    # query,SNPs
    # Consensus_39402_2#89.primertrimmed.consensus_threshold_0.75_quality_20,
    #
    # We only want to output a row if the sample actually has SNPs
    raw_nuc_mut_df = raw_nuc_mut_df.dropna()

    # https://stackoverflow.com/questions/13269890/cartesian-product-in-pandas
    # workaround for cartesian product in pandas < v1.2
    if raw_nuc_mut_df.shape[0] > 0:
        nuc_mut_df = raw_nuc_mut_df[["SNPs"]]
        nuc_mut_df['seqHash'] = seqHash
        # nuc_mut_df = (meta_df.assign(key=1)
        #                      .merge(
        #                         raw_nuc_mut_df[["SNPs"]].assign(key=1),
        #                         how="outer", on="key")
        #                      .drop("key", axis=1))
        
        split_mut_ser = nuc_mut_df["SNPs"].str.split("|", expand=True).stack()
        split_mut_ser = split_mut_ser.reset_index(drop=True, level=1)  # to line up with df's index
        split_mut_ser.name = "SNP"

        nuc_mut_df = nuc_mut_df.join(split_mut_ser)

        nuc_mut_df = nuc_mut_df.drop(columns=["SNPs"]).reset_index(drop=True)

    else:
        nuc_mut_df = pd.DataFrame(columns=["seqHash"] + ["SNP"])

        
    nuc_mut_df.to_csv(output_tsv, sep="\t", header=True, index=False)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='Calls amino acid substitutions, SNPs, nucleotide indels for a single sample.')

    parser.add_argument('--output_tsv', type=str,
                        help='Path to TSV containing mutation results.  ' +
                              'For nuc_indels mode, will output a file for insertions and another for deletions, ' +
                              'where filename is appended with ".insertions.tsv", ".deletions.tsv" respectively')
    parser.add_argument('--reference_fasta', type=str,
                        help='Path to reference fasta.  Required to call amino acid substitutions and SNPs')
    parser.add_argument('--reference_genbank', type=str,
                        help='Path to reference genbank.  Required to call amino acid substitutions')
    parser.add_argument('--sam', type=str,
                        help='Path to SAM file. Required to call amino acid substitutions, nucleotide indels')
    parser.add_argument('--mapped_fasta', type=str,
                        help='Path to aligned fasta with insertions removed and deletions padded.  Required to call SNPs')
    parser.add_argument('--mode', type=str,
                        help='One of [aa_mutations, nuc_mutations, nuc_indels] to ' +
                         'call amino acid substitutions (both nonsynonynous and synonymous), SNPs, ' +
                         'nucleotide insertions and deletions, respectively')
    parser.add_argument('--metadata_tsv', type=str,
                        help='Path to TSV containing metadata.  All output files will be prefixed with the metadata columns.')
    parser.add_argument('--threads', type=int, default=1,
                        help='Total threads used by gofasta.  Default="%(default)s"')


    args = parser.parse_args()
    if not (args.output_tsv and args.mode):
        parser.print_usage()
        sys.exit(1)

    metadata_tsv = args.metadata_tsv
    output_tsv = args.output_tsv
    reference_fasta = args.reference_fasta
    reference_genbank = args.reference_genbank
    mapped_fasta = args.mapped_fasta
    sam = args.sam
    mode = args.mode
    threads = args.threads

    if mode == MODE_AA_MUT:
        call_aa_mutations(metadata_tsv=metadata_tsv, output_tsv=output_tsv,
                          sam=sam, reference_fasta=reference_fasta,
                          reference_genbank=reference_genbank, threads=threads)
    elif mode == MODE_NUC_MUT:
        call_nuc_mutations(metadata_tsv=metadata_tsv, reference_fasta=reference_fasta,
                           mapped_fasta=mapped_fasta, output_tsv=output_tsv)
    elif mode == MODE_NUC_INDEL:
        call_nuc_indels(metadata_tsv=metadata_tsv, sam=sam, output_prefix=output_tsv)
    else:
        raise ValueError("Invalid mode.  Choose one of [{}]".format(", ".join([MODE_AA_MUT, MODE_NUC_MUT, MODE_NUC_INDEL])))