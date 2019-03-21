"""
Tool for converting quant data of various formats into HDF5 for RNAGET to use
TODO: only works for a few quant output file formats so far. See available subclasses.
"""
import h5py
import numpy as np
import csv
from datetime import datetime
import os

__MODE__ = None

# TODO: include an arg parser for this
__DATA_DIR__ = "/home/alipski/CanDIG/mock_data/rna_exp/"
__OUTPUT_FILE__ = "/home/alipski/CanDIG/mock_data/rna_exp/test_seq.h5"
__MAX_SAMPLES__ = 2000
__MAX_FEATURES__ = 75000


def get_file_list(path):
    #path = os.getcwd()
    return [file for file in os.listdir(path) if file.endswith(".tsv")]


class AbstractExpressionLoader(object):
    """
    Abstract class for loading expression data into an hdf5 store
    """

    # root
    exp_matrix_name = "expression"
    features_name = "axis/features"
    samples_name = "axis/samples"

    # metadata
    counts_name = "metadata/counts"
    features_len = "metadata/features_length"
    features_eff_len = "metadata/features_eff_length"

    def __init__(self, hdf5file, quantfilelist, study_id):
        self._filename = hdf5file
        self._quantfilelist = quantfilelist
        self._study_id = study_id
        self._units = None
        self._exp_col_name = None
        self._length_col_name = None
        self._gene_col_name = None
        self._counts_col_name = None
        self._feature_type = None
        self._source_file_type = None

    def _ingest_expressions(self):
        """
        TODO: improve storage of metadata and metadata bundles. Currently only using a bare minimum as POC
        :return: Put associated quant files into a single matrix along with available metadata
        """

        try:
            __MODE__ = 'r+'
            file = h5py.File(self._filename, __MODE__)

            # h5 directory: /
            if self.exp_matrix_name not in file:
                exp_matrix = self._create_float_matrix(file, self.exp_matrix_name)
                exp_matrix.attrs["pipeline"] = "unknown"
                exp_matrix.attrs["units"] = self._units
                exp_matrix.attrs["study"] = self._study_id
                exp_matrix.attrs["source_file_type"] = self._source_file_type
                i_qsize = 0
            else:
                exp_matrix = file[self.exp_matrix_name]
                i_qsize = exp_matrix.shape[0]

            if self.samples_name not in file:
                samples = file.create_dataset(
                    self.samples_name, (0,), maxshape=(__MAX_SAMPLES__,),
                    chunks=True, dtype="S20"
                )
                samples.attrs["created"] = str(datetime.now())

            else:
                samples = file[self.samples_name]
                if samples.attrs["source_file_type"] != self._source_file_type:
                    raise ValueError("Input file format incompatible with current hdf5 store.")

            # h5 directory: metadata/
            if self.counts_name not in file:
                counts_matrix = self._create_float_matrix(file, self.counts_name)
            else:
                counts_matrix = file[self.counts_name]

        except OSError as e:
            # No h5 file defined. Create new datasets

            __MODE__ = 'w'
            file = h5py.File(self._filename, __MODE__)
            # create expression matrix
            exp_matrix = self._create_float_matrix(file, self.exp_matrix_name)
            exp_matrix.attrs["pipeline"] = "unknown"
            exp_matrix.attrs["units"] = self._units
            exp_matrix.attrs["study"] = self._study_id
            exp_matrix.attrs["source_file_type"] = self._source_file_type
            i_qsize = 0

            if self.samples_name not in file:
                samples = file.create_dataset(
                    self.samples_name, (0,), maxshape=(__MAX_SAMPLES__,),
                    chunks=True, dtype="S20"
                )
                samples.attrs["created"] = str(datetime.now())

            else:
                samples = file[self.samples_name]
                if samples.attrs["source_file_type"] != self._source_file_type:
                    raise ValueError("Input file format incompatible with current hdf5 store.")

            # h5 directory: metadata/
            if self.counts_name not in file:
                counts_matrix = self._create_float_matrix(file, self.counts_name)
            else:
                counts_matrix = file[self.counts_name]

        q_index = i_qsize

        for quantfilename in self._quantfilelist:
            print(">>> Processing: "+quantfilename)

            with open(quantfilename, "r") as quantFile:
                quantificationReader = csv.reader(quantFile, delimiter="\t")
                sample_id = quantfilename.strip(".tsv")
                formatted_sample_id = str("PATIENT_"+sample_id).encode('utf8')

                # No duplicate sample ids in array
                if formatted_sample_id not in samples[...]:
                    expression_id = 0

                    header = next(quantificationReader)
                    exp_col_idx = self._set_column_index(header, self._exp_col_name)
                    quants = []
                    count_col_idx = self._set_column_index(header, self._counts_col_name)
                    counts = []

                    for expression in quantificationReader:
                        # expression level
                        quants.append(float(expression[exp_col_idx]))
                        counts.append(float(expression[count_col_idx]))
                        expression_id += 1

                    # expression matrix
                    exp_matrix.resize((exp_matrix.shape[0]+1, len(quants)))
                    exp_matrix[q_index] = [tuple(quants)]

                    # metadata matrix: counts
                    counts_matrix.resize((counts_matrix.shape[0]+1, len(counts)))
                    counts_matrix[q_index] = [tuple(counts)]

                    # associated array: samples
                    samples.resize((samples.shape[0]+1,))
                    samples[q_index] = formatted_sample_id

                    q_index += 1
                    file.flush()

                else:
                    print(">>> Error: duplicate sample")

            #else:
            #	raise Exception()

        file.close()

    def _ingest_features(self):
        """
        TODO:
        :return: Uses the first file as a reference file to create a feature label array for the HDF5 to use
        """

        # use first file in list as reference
        ref_file = self._quantfilelist[0]

        try:
            __MODE__ = 'r+'
            file = h5py.File(self._filename, __MODE__)
            if self.features_name in file:
                file.close()
                return
            else:
                features = self.setup_features_dataset(file)

        except OSError as e:
            print(">>> Creating new HDF5 store...")
            __MODE__ = 'w'
            file = h5py.File(self._filename, __MODE__)
            features = self.setup_features_dataset(file)

        with open(ref_file, "r") as quantFile:
            quantificationReader = csv.reader(quantFile, delimiter="\t")
            header = next(quantificationReader)
            gene_col_idx = self._set_column_index(header, self._gene_col_name)

            feature_list = []

            for expression in quantificationReader:
                feature_list.append(expression[gene_col_idx].encode('utf8'))

            features.resize((len(feature_list),))
            features[...] = feature_list
            file.flush()

        file.close()

    def build_hdf5(self):
        raise NotImplementedError( "Should have implemented this" )

    def setup_features_dataset(self, file):
        features = file.create_dataset(
            self.features_name, (__MAX_SAMPLES__,), maxshape=(__MAX_FEATURES__,),
            chunks=True, dtype="S20"
        )
        features.attrs["created"] = str(datetime.now())
        features.attrs["type"] = self._feature_type
        return features

    def reset_features(self):
        # TODO: implement
        # remove all features and feature-related metadata
        return

    def reset_expressions(self):
        # TODO: implement
        # remove all expression data and related metadata including samples labels
        return

    def reset(self):
        self.reset_features()
        self.reset_expressions()

    def _set_column_index(self, header, name):
        col_idx = None
        try:
            col_idx = header.index(name)
        except:
            if col_idx is None:
                raise KeyError("Missing {} column in expression table.".format(name))
        return col_idx

    def _set_all_columns(self, header):
        exp_col_idx = self._set_column_index(header,  self._exp_col_name)
        gene_col_idx = self._set_column_index(header, self._gene_col_name)
        counts_col_idx = self._set_column_index(header, self._counts_col_name)
        length_col_idx = self._set_column_index(header, self._length_col_name)

    def _create_float_matrix(self, h5file, matrix_name):
        new_matrix = h5file.create_dataset(
            matrix_name, (0,__MAX_FEATURES__),
            maxshape=(__MAX_SAMPLES__, __MAX_FEATURES__),
            chunks=True, dtype="f8"
        )
        new_matrix.attrs["created"] = str(datetime.now())
        new_matrix.attrs["row_label"] = self.samples_name
        new_matrix.attrs["col_label"] = self.features_name
        return new_matrix


class KallistoLoader(AbstractExpressionLoader):
    def __init__(self, hdf5file, quantfilelist, study_id, units="tpm", feature_type="gene"):
        super(KallistoLoader, self).__init__(
            hdf5file, quantfilelist, study_id)
        self._exp_col_name = "tpm"
        self._length_col_name = "eff_length"
        self._gene_col_name = "target_id"
        self._counts_col_name = "est_counts"
        self._units = units
        self._feature_type = feature_type
        self._source_file_type = "kallisto"

    def build_hdf5(self):
        self._ingest_features()
        self._ingest_expressions()


class CufflinksLoader(AbstractExpressionLoader):
    def __init__(self, hdf5file, quantfilelist, study_id, units="fpkm", feature_type="gene"):
        super(CufflinksLoader, self).__init__(
            hdf5file, quantfilelist, study_id)
        self._exp_col_name = "FPKM"
        self._gene_col_name = "tracking_id"
        self._units = units
        self._source_file_type = "cufflinks"
        self._feature_type = feature_type
        # TODO: support more fields?

    def build_hdf5(self):
        raise NotImplementedError("not done yet")


class RSEMLoader(AbstractExpressionLoader):
    def __init__(self, hdf5file, quantfilelist, study_id, units="tpm", feature_type="gene"):
        super(RSEMLoader, self).__init__(
            hdf5file, quantfilelist, study_id)
        self._exp_col_name = "TPM"
        # TODO: gene_col_name could be a transcript_id
        if feature_type == "transcript":
            self._gene_col_name = "transcript_id"
        else:
            self._gene_col_name = "gene_id"
        self._counts_col_name = "expected_count"
        self._units = units
        self._source_file_type = "rsem"
        # TODO: support more fields?

    def build_hdf5(self):
        raise NotImplementedError("not done yet")


class MatrixLoader(AbstractExpressionLoader):
    """
    Upload a full csv matrix?
    """
    def __init__(self, hdf5file, quantfilelist, study_id, file_format, units="tpm", feature_type="gene"):
        super(MatrixLoader, self).__init__(
            hdf5file, quantfilelist, study_id)
        self._units = units
        self._feature_type = feature_type

    def build_hdf5(self):
        raise NotImplementedError("not sure if will do yet")


if __name__ == "__main__":
    file_list = get_file_list(__DATA_DIR__)
    # testing ingest with kallisto format for now
    hdf5_expression = KallistoLoader(__OUTPUT_FILE__, file_list, "pog")
    hdf5_expression.build_hdf5()
