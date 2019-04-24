"""
HDF5 Query tool
"""
import h5py
import numpy as np
import flask
from datetime import datetime

app = flask.current_app
SUPPORTED_OUTPUT_FORMATS = [".json",".h5"]  # ".loom"]


class ExpressionQueryTool(object):
    """
    Supports searching expression data from properly formatted HDF5 files
        - sample id
        - features by list:
            - feature accession (soon)
            - feature name (soon)
            - feature id
        - expression threshold array:
            - either minThreshold or maxThreshold
            - [(feature1, threshold1), (feature2, theshold2)...]
    """

    def __init__(self, input_file, output_file, include_metadata=False, output_type=".h5"):
        """

        :param hdf5path: path to HDF5 file being queried
        :param include_metadata: (bool) include expression file metadata in results
        :param output: JSON for now, plan to HDF5,loom,and others soon
        """
        self._file = h5py.File(input_file, 'r')
        self._output_file = output_file
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
        indices = {}
        encoded_list = list(map(str.encode, id_list))
        arr = self._file[axis][:]
        for encoded_id in encoded_list:
            lookup = np.where(arr == encoded_id)[0]
            if len(lookup) > 0:
                indices[encoded_id.decode()] = lookup[0]
        return indices

    def _search_sample(self, sample_id):
        # TODO: support multiple sample_id list searches at some point?
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

            if self._include_metadata:
                counts = self.get_raw_counts()
                sample_counts = counts[sample_index,...]

                if self._output_format == ".json":
                    results["metadata"]["raw_counts"][sample_id] = list(sample_counts)

                elif self._output_format == ".h5":
                    results = self._write_hdf5_metadata(
                        results, 1, len(feature_load), counts=sample_counts)

        return results

    def _search_features(self, feature_list, ft_list=None, ft_type=None):
        """
        feature_list must be a list of gene ID valid to data set
        """
        expression = self.get_expression_matrix()
        samples = self.get_samples()
        indices = self._get_hdf5_indices(self._features, feature_list)
        results = self._build_results_template(expression)
        feature_expressions = []
        sample_expressions =[]

        if self._include_metadata:
            counts = self.get_raw_counts()
            feature_counts = []

        if ft_list and ft_type:
            samples_list = []

            if ft_type == "min":
                def ft_compare(x,y):
                    return x > y
            else:
                def ft_compare(x,y):
                    return x <= y

            if self._output_format == ".json":
                results["features"] = list(feature_list)

            for idx, sample in enumerate(expression):
                include = True
                threshold_samples = []

                if self._include_metadata:
                    threshold_counts = []

                for feature_id, threshold in ft_list:
                    feature_index = indices[feature_id]

                    if not ft_compare(expression[idx][feature_index], threshold):
                        include = False
                        break
                    else:
                        threshold_samples.append(sample[feature_index])

                        if self._include_metadata:
                            threshold_counts.append(counts[idx][feature_index])

                if include:
                    sample_expressions.append(threshold_samples)
                    samples_list.append(samples[idx].decode())

                    if self._include_metadata:
                        feature_counts.append(threshold_counts)

        else:
            for feature_id in indices:
                if self._output_format == ".json":
                    results["features"].append(feature_id)
                slice_index = indices[feature_id]

                feature_expressions.append(list(expression[...,slice_index]))

                if self._include_metadata:
                    feature_counts.append(list(counts[...,slice_index]))

            samples_list = list(map(bytes.decode, self.get_samples()))

            if self._include_metadata:
                feature_counts = np.transpose(feature_counts)

        if self._output_format == ".json":
            if feature_expressions:
                feature_zip = zip(*feature_expressions)
                results["expression"] = dict(zip(samples_list, (map(list, feature_zip))))
            else:
                results["expression"] = dict(zip(samples_list, sample_expressions))

            if self._include_metadata:
                counts_zip = zip(*feature_counts)
                results["metadata"]["raw_counts"] = dict(zip(samples_list, (map(list,counts_zip))))

        elif self._output_format == ".h5":
            encoded_samples = [sample.encode('utf-8') for sample in samples_list]
            encoded_features = [feature.encode('utf-8') for feature in indices]

            if feature_expressions:
                results = self._write_hdf5_results(
                    results, encoded_samples, encoded_features, np.transpose(feature_expressions))

            elif sample_expressions:
                results = self._write_hdf5_results(
                    results, encoded_samples, encoded_features, sample_expressions)

            if self._include_metadata:
                results = self._write_hdf5_metadata(
                    results, len(encoded_samples), len(encoded_features), counts=feature_counts)

        return results

    def search(self, sample_id=None, feature_list=None):
        """
        General search function. Accepts any combination of the following arguments:
        - sample_id || feature_list || sample_id and feature_list
        """
        if sample_id and feature_list:
            expression = self.get_expression_matrix()
            sample_index = self._get_hdf5_indices(self._samples, [sample_id]).get(sample_id)
            feature_indices = self._get_hdf5_indices(self._features, feature_list)
            results = self._build_results_template(expression)

            if sample_index is not None:
                feature_expressions = []
                sample_expressions = expression[sample_index,...]

                if self._include_metadata:
                    counts = self.get_raw_counts()
                    sample_counts = counts[sample_index,...]
                    feature_counts = []

                for feature_id in feature_indices:
                    if self._output_format == ".json":
                        results["features"].append(feature_id)
                    slice_index = feature_indices[feature_id]
                    feature_expressions.append(sample_expressions[slice_index])

                    if self._include_metadata:
                        feature_counts.append(sample_counts[slice_index])

                if self._output_format == ".json":
                    results["expression"][sample_id] = feature_expressions
                    if self._include_metadata:
                        results["metadata"]["raw_counts"][sample_id] = feature_counts

                elif self._output_format == ".h5":
                    encoded_samples = [sample_id.encode('utf8')]
                    encoded_features = [feature.encode('utf-8') for feature in feature_indices]

                    results = self._write_hdf5_results(
                        results, encoded_samples, encoded_features, np.transpose(feature_expressions))

                    if self._include_metadata:
                        results = self._write_hdf5_metadata(
                            results, len(encoded_samples), len(encoded_features), counts=np.transpose(feature_counts))

            return results

        elif sample_id:
            return self._search_sample(sample_id)

        elif feature_list:
            return self._search_features(feature_list)

        else:
            raise ValueError("Invalid argument values provided")

    def search_threshold(self, ft_list, ft_type="min"):
        feature_list = list(zip(*ft_list))[0]
        return self._search_features(feature_list, ft_list=ft_list, ft_type=ft_type)

    def _write_hdf5_results(self, results, sample_list, feature_list, expressions):
        """

        :param results: the results file-object being written to
        :param sample_list: samples list
        :param feature_list: features list
        :param expressions: quantification matrix values
        :return:
        """
        features_ds = results.create_dataset(
            self._features, (len(feature_list),), maxshape=(len(feature_list),), dtype="S20")
        features_ds[...] = feature_list

        samples_ds = results.create_dataset(
            self._samples, (len(sample_list),), maxshape=(len(sample_list),), dtype="S20")
        samples_ds[...] = sample_list

        expression_ds = results.create_dataset(
            self._expression_matrix, (len(sample_list), len(feature_list)), maxshape=(len(sample_list),len(feature_list)), chunks=True, dtype="f8")

        expression_ds.attrs["units"] = self.get_expression_matrix().attrs.get("units")
        expression_ds.attrs["created"] = str(datetime.now())
        expression_ds.attrs["row_label"] = self._samples
        expression_ds.attrs["col_label"] = self._features
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
                self._counts, (num_samples, num_features), maxshape=(num_samples, num_features), chunks=True, dtype="f8")

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
