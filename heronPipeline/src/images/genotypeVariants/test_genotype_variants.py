"""
Unit test genotype-variants.py
"""


import unittest
import sys
import subprocess
import os
import datetime
import csv


CURR_DIR = os.path.dirname(os.path.realpath(__file__))



EXPECTED_GENOTYPES_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


class TestGenotypeVariants(unittest.TestCase):



    def test_genotype_variants(self):
        """
        AS any user with execute rights on genotype-variants.py
        WHEN I execute genotype-variants.py with a wuhan aligned fasta
            with deletions padded and insertions removed using PHE rules
        THEN it assigns the correct voc/vui classification and confidence.

        The test set should contain one sample from each combination of
            (voc/vui, confidence).
        """
        exp_sample_tsv = os.path.join(CURR_DIR, "assets", "voc-vui-test-samples.tsv")
        with open(exp_sample_tsv, 'r', newline='') as fh_in:
            reader = csv.DictReader(fh_in, delimiter="\t")
            for row in reader:
                print("Starting next row")
                # Skip commented out rows
                if row["coguk_id"].startswith("#"):
                    continue

                # test fasta name:  [coguk_id]_[run]_[lane]_[tag].mapped.fa
                fasta_basename = (row["coguk_id"] + "_" +
                                  row["run"] + "_" +
                                  row["lane"] + "_" +
                                  row["tag"] +
                                  ".mapped.fa")

                print(f"Processing fasta: {fasta_basename}")
                fasta_path  = os.path.join(CURR_DIR, "assets", "fasta", "align", fasta_basename)

                # python genotype-variants.py [fasta] phe_recipes.yml --verbose
                cmd = ["python", os.path.join(CURR_DIR, "genotype-variants.py"),
                        fasta_path,
                        os.path.join(CURR_DIR, "phe-recipes.yml"), "--verbose"]

                # check:  check error code
                # captpure_output:  capture stdout
                # text:  capture stout as text
                proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
                if (proc.stderr.strip()):
                    print (f"Error: {proc.stderr}")
                self.assertIsNotNone(proc.stdout)
                self.assertTrue(len(proc.stdout.split("\t")) == 4, "Sample {} {} {} {} wrong number of output={}.\nCMD: {}".format(
                                    row["coguk_id"], row["run"], row["lane"], row["tag"],
                                    proc.stdout,
                                    " ".join(cmd)
                                ))
                act_voc_profile, act_voc_vui, act_confidence, act_timestamp = proc.stdout.strip().split("\t")
                print(f"{fasta_basename}: {act_voc_profile}, {act_voc_vui}, {act_confidence}, {act_timestamp}")

                exp_voc_profile = row["voc_profile"]
                exp_voc_vui = row["voc"]
                exp_confidence = row["confidence"]

                self.assertEqual(exp_voc_profile, act_voc_profile,
                    "Sample {} {} {} {} expected voc/vui profile={} but got {}.\nCMD: {}".format(
                        row["coguk_id"], row["run"], row["lane"], row["tag"],
                        exp_voc_profile,
                        act_voc_profile,
                        " ".join(cmd)
                    ))

                self.assertEqual(exp_voc_vui, act_voc_vui,
                    "Sample {} {} {} {} expected voc/vui={} but got {}.\nCMD: {}".format(
                        row["coguk_id"], row["run"], row["lane"], row["tag"],
                        exp_voc_vui,
                        act_voc_vui,
                        " ".join(cmd)
                    ))

                self.assertEqual(exp_confidence, act_confidence,
                    "Sample {} {} {} {} expected confidence={} but got {}.\nCMD: {}".format(
                        row["coguk_id"], row["run"], row["lane"], row["tag"],
                        exp_confidence,
                        act_confidence,
                        " ".join(cmd)
                    ))
                print(f"{fasta_basename} is good")
                try:
                    datetime.datetime.strptime(act_timestamp, EXPECTED_GENOTYPES_TIMESTAMP_FORMAT)
                except:
                    self.fail("Sample {} {} {} {} expected timestamp format={} but got {}.\nCMD: {}".format(
                        row["coguk_id"], row["run"], row["lane"], row["tag"],
                        EXPECTED_GENOTYPES_TIMESTAMP_FORMAT,
                        act_timestamp,
                        " ".join(cmd)
                    ))



if __name__ == '__main__':
    unittest.main()
