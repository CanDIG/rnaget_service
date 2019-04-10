"""
Tool for converting quant data of various formats into HDF5 for RNAGET to use
TODO: only works for a few quant output file formats so far. See available subclasses.
"""
import h5py
import numpy as np
import csv
from datetime import datetime
import os

__MAX_SAMPLES__ = 2000
__MAX_FEATURES__ = 75000


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

    def __init__(self, hdf5file, datadir, study_id):
        try:
            self.__MODE__ = 'r+'
            self._file = h5py.File(hdf5file, self.__MODE__)
        except OSError as e:
            print(">>> Creating new HDF5 store...")
            self.__MODE__ = 'w'
            self._file = h5py.File(hdf5file, self.__MODE__)

        self._datadir = datadir
        self._quantfilelist = []
        self._study_id = study_id
        self._units = None
        self._exp_col_name = None
        self._length_col_name = None
        self._gene_col_name = None
        self._counts_col_name = None
        self._feature_type = None
        self._source_file_type = None
        self._headers = True
        self._file_ext = None

    def _ingest_expressions(self):

        if self.__MODE__ == 'r+':

            # h5 directory: /
            if self.exp_matrix_name not in self._file:
                exp_matrix = self._create_float_matrix(self._file, self.exp_matrix_name)
                exp_matrix.attrs["pipeline"] = "unknown"
                exp_matrix.attrs["units"] = self._units
                exp_matrix.attrs["study"] = self._study_id
                exp_matrix.attrs["source_file_type"] = self._source_file_type
                i_qsize = 0
            else:
                exp_matrix = self._file[self.exp_matrix_name]
                i_qsize = exp_matrix.shape[0]

            if self.samples_name not in self._file:
                samples = self._file.create_dataset(
                    self.samples_name, (0,), maxshape=(__MAX_SAMPLES__,),
                    chunks=True, dtype="S20"
                )
                samples.attrs["created"] = str(datetime.now())

            else:
                samples = self._file[self.samples_name]
                if exp_matrix.attrs["source_file_type"] != self._source_file_type:
                    raise ValueError("Input file format incompatible with current hdf5 store.")

            # h5 directory: metadata/
            if self.counts_name not in self._file:
                counts_matrix = self._create_float_matrix(self._file, self.counts_name)
            else:
                counts_matrix = self._file[self.counts_name]

        else:
            # __MODE__ == 'w'

            exp_matrix = self._create_float_matrix(self._file, self.exp_matrix_name)
            exp_matrix.attrs["pipeline"] = "unknown"
            exp_matrix.attrs["units"] = self._units
            exp_matrix.attrs["study"] = self._study_id
            exp_matrix.attrs["source_file_type"] = self._source_file_type
            i_qsize = 0

            if self.samples_name not in self._file:
                samples = self._file.create_dataset(
                    self.samples_name, (0,), maxshape=(__MAX_SAMPLES__,),
                    chunks=True, dtype="S20"
                )
                samples.attrs["created"] = str(datetime.now())

            else:
                samples = self._file[self.samples_name]
                if samples.attrs["source_file_type"] != self._source_file_type:
                    raise ValueError("Input file format incompatible with current hdf5 store.")

            # h5 directory: metadata/
            if self.counts_name not in self._file:
                counts_matrix = self._create_float_matrix(self._file, self.counts_name)
            else:
                counts_matrix = self._file[self.counts_name]

        q_index = i_qsize

        for quantfilename in self._quantfilelist:
            print(">>> Processing: "+quantfilename)

            with open(self._datadir+quantfilename, "r") as q_file:
                quantificationReader = csv.reader(q_file, delimiter="\t")
                formatted_sample_id = self.get_sample_id(quantfilename)

                # No duplicate sample ids in array
                if formatted_sample_id not in samples[...]:
                    expression_id = 0

                    header = None
                    if self._headers:
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
                    self._file.flush()

                else:
                    print(">>> Error: duplicate sample")

            #else:
            #   raise Exception()

    def _ingest_features(self):

        # use first file in list as reference
        ref_file = self._datadir+self._quantfilelist[0]

        if self.__MODE__ == 'r+':
            if self.features_name in self._file:
                return
            else:
                features = self.setup_features_dataset(self._file)

        else:
            # __MODE__ = 'w'
            features = self.setup_features_dataset(self._file)

        with open(ref_file, "r") as q_file:
            quantificationReader = csv.reader(q_file, delimiter="\t")
            header = None
            if self._headers:
                header = next(quantificationReader)
            gene_col_idx = self._set_column_index(header, self._gene_col_name)

            feature_list = []

            for expression in quantificationReader:
                feature_list.append(expression[gene_col_idx].encode('utf8'))

            features.resize((len(feature_list),))
            features[...] = feature_list
            self._file.flush()

    def build_hdf5(self):
        raise NotImplementedError( "Should have implemented this" )

    def get_file_list(self, datadir):
        """
        TODO: arg parsing to specificy a directory of files to ingest?
        """
        if datadir:
            path = datadir
        else:
            path = os.getcwd()
        file_list = [file for file in os.listdir(path) if file.endswith(self._file_ext)]

        if not file_list:
            raise ValueError("No files found with appropriate file extension: {}".format(self._file_ext))
        else:
            return file_list

    def get_sample_id(self, quantfilename):
        sample_id = quantfilename.strip(self._file_ext)
        return str(sample_id).encode('utf8')

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
        if header:
            try:
                col_idx = header.index(name)
            except:
                if col_idx is None:
                    raise KeyError("Missing {} column in expression table.".format(name))
        else:
            col_idx = int(name)
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
    def __init__(self, hdf5file, datadir, study_id, units="tpm", feature_type="gene"):
        super(KallistoLoader, self).__init__(
            hdf5file, datadir, study_id)
        self._exp_col_name = "tpm"
        self._length_col_name = "eff_length"
        self._gene_col_name = "target_id"
        self._counts_col_name = "est_counts"
        self._units = units
        self._feature_type = feature_type
        self._source_file_type = "kallisto"
        self._file_ext = ".tsv"
        self._quantfilelist = self.get_file_list(datadir)

    def build_hdf5(self):
        self._ingest_features()
        self._ingest_expressions()
        self._file.close()


class CufflinksLoader(AbstractExpressionLoader):
    def __init__(self, hdf5file, datadir, study_id, units="fpkm", feature_type="gene"):
        super(CufflinksLoader, self).__init__(
            hdf5file, datadir, study_id)
        self._exp_col_name = "FPKM"
        self._gene_col_name = "tracking_id"
        self._units = units
        self._source_file_type = "cufflinks"
        self._feature_type = feature_type
        self._file_ext = ".tsv"
        self._quantfilelist = self.get_file_list(datadir)
        # TODO: support more fields?


class RSEMLoader(AbstractExpressionLoader):
    def __init__(self, hdf5file, datadir, study_id, units="tpm", feature_type="gene"):
        super(RSEMLoader, self).__init__(
            hdf5file, datadir, study_id)
        self._exp_col_name = "TPM"
        # TODO: gene_col_name could be a transcript_id
        if feature_type == "transcript":
            self._gene_col_name = "transcript_id"
        else:
            self._gene_col_name = "gene_id"
        self._counts_col_name = "expected_count"
        self._units = units
        self._source_file_type = "rsem"
        self._file_ext = ".tsv"
        self._quantfilelist = self.get_file_list(datadir)
        # TODO: support more fields?


class GSCLoader(AbstractExpressionLoader):
    """
    Used for loading GSC pipeline expression quant data
    file extension used: stranded_collapsed_coverage.transcript.normalized
    """
    def __init__(self, hdf5file, datadir, study_id, units="rpkm", feature_type="gene"):
        super(GSCLoader, self).__init__(
            hdf5file, datadir, study_id)
        self._headers = False
        self._units = units
        self._feature_type = feature_type
        self._source_file_type = "gsc pipeline"
        self._gene_col_name = 0
        self._exp_col_name = 14
        self._counts_col_name = 9
        self._file_ext = "stranded_collapsed_coverage.transcript.normalized"
        self._quantfilelist = self.get_file_list(datadir)

    def get_sample_id(self, quantfilename):
        sample_id = quantfilename.split("_",1)[0]
        return str(sample_id).encode("utf8")

    def build_hdf5(self):
        self._ingest_features()
        self._ingest_expressions()
        self._file.close()


# some tests
if __name__ == "__main__":
    __OUTPUT_FILE__ = "/home/alipski/CanDIG/mock_data/rna_exp/gsc_exp_test.h5"
    __DATA_DIR__ = "/home/alipski/CanDIG/mock_data/rna_exp/gsc_examples/exp_examples/"
    #test_hdf5_expression = KallistoLoader(__OUTPUT_FILE__, __DATA_DIR__, "pog")
    test_hdf5_expression = GSCLoader(__OUTPUT_FILE__, __DATA_DIR__, "pog")
    test_hdf5_expression.build_hdf5()
