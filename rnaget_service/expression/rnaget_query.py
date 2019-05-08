"""
HDF5 Query tool
"""
import h5py
import numpy as np
import flask
import pandas as pd
from collections import OrderedDict

app = flask.current_app
SUPPORTED_OUTPUT_FORMATS = [".json",".h5"]  # ".loom"]


class ExpressionQueryTool(object):
    """
    Supports searching expression data from properly formatted HDF5 files
        - sample id
        - features by list:
            - feature accession
            - feature name
            - feature id
        - expression threshold array:
            - either minThreshold or maxThreshold
            - [(feature1, threshold1), (feature2, theshold2)...]
    """

    def __init__(self, input_file, output_file=None, include_metadata=False, output_type=".h5", feature_map=None):
        """

        :param input_file: path to HDF5 file being queried
        :param output_file: path where output file should be generated (for non JSON output types)
        :param include_metadata: (bool) include expression file metadata in results
        :param output_type: .json | .h5 (.loom in progress)
        :param feature_map: .tsv file containing mapping for gene_id<->gene_name<->accessions
        """
        self._file = h5py.File(input_file, 'r')
        self._output_file = output_file
        self._feature_map = feature_map
        self._expression_matrix = "expression"
        self._features = "axis/features"
        self._samples = "axis/samples"
        self._counts = "metadata/counts"
        self._include_metadata = include_metadata

        if output_type not in SUPPORTED_OUTPUT_FORMATS:
            raise ValueError("Valid output formats are: {}".format(SUPPORTED_OUTPUT_FORMATS))
        else:
            self._output_format = output_type

    def get_features(self):
        """
        :return HDF5 features array data set
        """
        return self._file[self._features]

    def get_samples(self):
        """
        :return HDF5 samples array data set
        """
        return self._file[self._samples]

    def get_expression_matrix(self):
        """
        :return HDF5 expression matrix data set
        """
        return self._file[self._expression_matrix]

    def get_raw_counts(self):
        """
        :return HDF5 raw counts metadata set
        """
        return self._file[self._counts]

    def _get_hdf5_indices(self, axis, id_list):
        """

        :param axis: axis data set from hdf5 file
        :param id_list: list of id's to convert into indices
        :return: an ordered dict of id,index pairs (from index low->high)
        """
        indices = {}
        encoded_list = list(map(str.encode, id_list))
        arr = self._file[axis][:]
        for encoded_id in encoded_list:
            lookup = np.where(arr == encoded_id)[0]
            if len(lookup) > 0:
                indices[encoded_id.decode()] = lookup[0]
        sorted_indices = sorted(indices.items(), key=lambda i: i[1])
        return OrderedDict([(k,v) for (k,v) in sorted_indices])

    def _search_sample(self, sample_id):
        expression = self.get_expression_matrix()
        sample_index = self._get_hdf5_indices(self._samples, [sample_id]).get(sample_id)

        results = self._build_results_template(expression)

        if sample_index is not None:
            sample_expressions = expression[sample_index,...]
            feature_load = self.get_features()[...]

            if self._output_format == ".json":
                results["features"] = list(map(bytes.decode, feature_load))
                results["expression"][sample_id] = list(sample_expressions)

            elif self._output_format == ".h5":
                encoded_samples = [sample_id.encode('utf8')]

                results = self._write_hdf5_results(
                    results, encoded_samples, feature_load, sample_expressions)

                results[self._expression_matrix].attrs["units"] = expression.attrs.get("units")

            if self._include_metadata:
                counts = self.get_raw_counts()
                sample_counts = counts[sample_index,...]

                if self._output_format == ".json":
                    results["metadata"]["raw_counts"][sample_id] = list(sample_counts)

                elif self._output_format == ".h5":
                    results = self._write_hdf5_metadata(
                        results, 1, len(feature_load), counts=sample_counts)

        return results

    def _search_features(self, feature_list, ft_list=None, ft_type=None, supplementary_feature_label=None):
        """
        feature_list must be a list of gene ID valid to data set
        """
        expression = self.get_expression_matrix()
        samples = self.get_samples()
        indices = self._get_hdf5_indices(self._features, feature_list)
        results = self._build_results_template(expression)
        counts = None
        feature_expressions = []
        sample_expressions = []
        supplementary_feature_array = None

        if supplementary_feature_label:
            supplementary_feature_array = []
            for feature_id in indices:
                supplementary_feature_array.append(supplementary_feature_label[feature_id])

        if self._include_metadata:
            counts = self.get_raw_counts()

        feature_slices = list(indices.values())

        if ft_list and ft_type:
            if ft_type == "min":
                def ft_compare(x,y):
                    return x > y
            else:
                def ft_compare(x,y):
                    return x <= y

            if self._output_format == ".json":
                results["features"] = list(feature_list)

            # read slices in to memory as a data frame
            expression_df = pd.DataFrame(expression[:,feature_slices])

            # slice data frame while samples remain
            for feature_id, threshold in ft_list:
                if len(expression_df) > 0:
                    df_feature_index = list(indices.keys()).index(feature_id)
                    expression_df = expression_df[ft_compare(expression_df[df_feature_index],threshold)]
                else:
                    break

            sample_expressions = expression_df.values
            samples_list = [samples[idx].decode() for idx in expression_df.index]

        else:
            if self._output_format == ".json":
                results["features"] = list(indices.keys())

            feature_expressions = list(expression[:, feature_slices])
            samples_list = list(map(bytes.decode, self.get_samples()))

        if self._output_format == ".json":
            if feature_expressions:
                feature_zip = zip(*feature_expressions)
                results["expression"] = dict(zip(samples_list, (map(list, feature_zip))))
            else:
                results["expression"] = dict(zip(samples_list, sample_expressions))

            if self._include_metadata:
                feature_counts = list(counts[:, feature_slices])
                counts_zip = zip(*feature_counts)
                results["metadata"]["raw_counts"] = dict(zip(samples_list, (map(list,counts_zip))))

        elif self._output_format == ".h5":
            encoded_samples = [sample.encode('utf-8') for sample in samples_list]
            encoded_features = [feature.encode('utf-8') for feature in indices]

            if len(feature_expressions)>0:
                results = self._write_hdf5_results(
                    results, encoded_samples, encoded_features, feature_expressions,
                    suppl_features_label=supplementary_feature_array)

            elif len(sample_expressions)>0:
                results = self._write_hdf5_results(
                    results, encoded_samples, encoded_features, sample_expressions,
                    suppl_features_label=supplementary_feature_array)

            if self._include_metadata:
                feature_counts = list(counts[:, feature_slices])
                results = self._write_hdf5_metadata(
                    results, len(encoded_samples), len(encoded_features), counts=feature_counts)

        return results

    def search(self, sample_id=None, feature_list_id=None, feature_list_accession=None, feature_list_name=None):
        """
        General search function. Accepts any combination of the following arguments:
        - sample_id || feature_list_id or feature_list_accession or feature_list_name || sample_id and feature_list
        """
        supplementary_feature_label = {}
        feature_counts = None

        if feature_list_accession or feature_list_name:
            if feature_list_id or (feature_list_accession and feature_list_name):
                raise ValueError("Invalid argument values provided")
            df = pd.read_csv(self._feature_map, sep='\t')
            feature_list_id = []

            if feature_list_name:
                df_key = 'gene_symbol'
                feature_list = feature_list_name
            else:
                df_key = 'accession_numbers'
                feature_list = feature_list_accession

            for feature_value in feature_list:
                df_lookup = df.loc[df[df_key] == feature_value].ensembl_id
                if len(df_lookup) > 0:
                    gene_id = df_lookup.values[0]
                    feature_list_id.append(gene_id)
                    supplementary_feature_label[gene_id] = feature_value

            for sf in supplementary_feature_label:
                supplementary_feature_label[sf] = supplementary_feature_label[sf].encode('utf-8')

        if sample_id and feature_list_id:
            expression = self.get_expression_matrix()
            sample_index = self._get_hdf5_indices(self._samples, [sample_id]).get(sample_id)
            feature_indices = self._get_hdf5_indices(self._features, feature_list_id)
            results = self._build_results_template(expression)
            supplementary_feature_array = None

            if supplementary_feature_label:
                supplementary_feature_array = []
                for feature_id in feature_indices:
                    supplementary_feature_array.append(supplementary_feature_label[feature_id])

            if sample_index is not None:
                feature_slices = list(feature_indices.values())
                feature_expressions = list(expression[sample_index, feature_slices])

                if self._include_metadata:
                    counts = self.get_raw_counts()
                    feature_counts = list(counts[sample_index, feature_slices])

                if self._output_format == ".json":
                    results["features"] = list(feature_indices.keys())
                    results["expression"][sample_id] = feature_expressions
                    if self._include_metadata:
                        results["metadata"]["raw_counts"][sample_id] = feature_counts

                elif self._output_format == ".h5":
                    encoded_samples = [sample_id.encode('utf8')]
                    encoded_features = [feature.encode('utf-8') for feature in feature_indices]

                    results = self._write_hdf5_results(
                        results, encoded_samples, encoded_features, feature_expressions,
                        suppl_features_label=supplementary_feature_array)

                    if self._include_metadata:
                        results = self._write_hdf5_metadata(
                            results, len(encoded_samples), len(encoded_features), counts=feature_counts)

            return results

        elif sample_id:
            return self._search_sample(sample_id)

        elif feature_list_id:
            return self._search_features(feature_list_id, supplementary_feature_label=supplementary_feature_label)

        else:
            raise ValueError("Invalid argument values provided")

    def search_threshold(self, ft_list, ft_type="min", feature_label="name"):
        feature_list = list(zip(*ft_list))[0]
        supplementary_feature_label = {}

        if feature_label in ["id", "name", "accession"]:
            feature_list_id = []
            if feature_label == "id":
                feature_list_id = feature_list
            else:
                df = pd.read_csv(self._feature_map, sep='\t')
                ft_list_id = []
                if feature_label == "name":
                    df_key = "gene_symbol"
                else:
                    df_key = "accession_numbers"
                for feature, threshold in ft_list:
                    df_lookup = df.loc[df[df_key] == feature].ensembl_id
                    if len(df_lookup) > 0:
                        gene_id = df_lookup.values[0]
                        feature_list_id.append(gene_id)
                        ft_list_id.append((gene_id, threshold))
                        supplementary_feature_label[gene_id] = feature

                ft_list = ft_list_id

                for sf in supplementary_feature_label:
                    supplementary_feature_label[sf] = supplementary_feature_label[sf].encode('utf-8')
        else:
            raise ValueError("Invalid feature label values provided")

        return self._search_features(
            feature_list_id, ft_list=ft_list, ft_type=ft_type, supplementary_feature_label=supplementary_feature_label
        )

    def _write_hdf5_results(self, results, sample_list, feature_list, expressions,
                            suppl_features_label=None, transpose=False):
            features_ds = results.create_dataset(
                self._features, (len(feature_list),1), maxshape=(len(feature_list),2), dtype="S20")
            features_data = [feature_list]

            if suppl_features_label:
                features_ds.resize((len(feature_list),2))
                features_data.append(suppl_features_label)

            features_ds[...] = np.transpose(features_data)

            samples_ds = results.create_dataset(
                self._samples, (len(sample_list),), maxshape=(len(sample_list),), dtype="S20")
            samples_ds[...] = sample_list

            expression_ds = results.create_dataset(
                self._expression_matrix, (len(sample_list), len(feature_list)),
                maxshape=(len(sample_list),len(feature_list)), chunks=True, dtype="f8")

            ref_ds = self.get_expression_matrix()
            expression_ds.attrs["units"] = ref_ds.attrs.get("units")
            expression_ds.attrs["study"] = ref_ds.attrs.get("study")

            if transpose:
                expression_ds[...] = np.transpose(expressions)
            else:
                expression_ds[...] = expressions

            return results

    def _write_hdf5_metadata(self, results, num_samples, num_features, counts=None):
        """
        Write metadata matrices to file
        :param results: the results file-object being written to
        :return:
        """
        if counts is not None:
            counts_ds = results.create_dataset(
                self._counts, (num_samples, num_features),
                maxshape=(num_samples, num_features), chunks=True, dtype="f8")

            counts_ds[...] = counts

        return results

    @staticmethod
    def split_column(col_array):
        """
        Splits a single column result into an array of arrays
        *** response formatting helper to keep possibility of multi-sample queries open
        """
        col_split = np.array_split(col_array,len(col_array))
        return list(map(list, col_split))

    def _build_results_template(self, expression_matrix):
        """
        Construct a results object template from the expression dataset
        """
        if self._output_format == '.json':
            results = {
                "expression": {},
                "features": []
            }

            if self._include_metadata:
                # TODO: default metadata fields? so far units only required
                results["metadata"] = {
                    "units": expression_matrix.attrs.get("units"),
                    "raw_counts": {}
                }

        elif self._output_format == '.h5':
            file_path = self._output_file
            results = h5py.File(file_path, 'w', driver='core')
        else:
            results = None

        return results

    def close(self):
        self._file.close()
