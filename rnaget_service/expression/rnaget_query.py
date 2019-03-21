"""
HDF5 Query tool
"""
import h5py
import numpy as np
import json
import flask

app = flask.current_app
SUPPORTED_OUTPUT_FORMATS = ["json"] # , "hdf5", "loom"]
# testing only
__TEST_DIR__ = '/home/alipski/CanDIG/mock_data/rna_exp/test_seq.h5'


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

    def __init__(self, hdf5path, include_metadata=False, output="json"):
        """

        :param hdf5path: path to HDF5 file being queried
        :param include_metadata: (bool) include expression file metadata in results
        :param output: JSON for now, plan to HDF5,loom,and others soon
        """
        self._file = h5py.File(hdf5path, 'r')
        self._expression_matrix = "expression"
        self._features = "axis/features"
        self._samples = "axis/samples"
        self._counts = "metadata/counts"
        self._output_format = output
        self._include_metadata = include_metadata

    def get_features(self):
        """
        :return HDF5 features array dataset
        """
        return self._file[self._features]

    def get_samples(self):
        """
        :return HDF5 samples array dataset
        """
        return self._file[self._samples]

    def get_expression_matrix(self):
        """
        :return HDF5 expression matrix dataset
        """
        return self._file[self._expression_matrix]

    def get_raw_counts(self):
        """
        :return HDF5 raw counts metadataset
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
            results["features"] = list(map(bytes.decode, feature_load))
            results["expression"][sample_id] = list(sample_expressions)

            if self._include_metadata:
                counts = self.get_raw_counts()
                sample_counts = counts[sample_index,...]
                results["metadata"]["raw_counts"][sample_id] = list(sample_counts)

        return results

    def _search_features(self, feature_list):
        """
        feature_list must be a list of gene ID valid to dataset
        """
        expression = self.get_expression_matrix()
        indices = self._get_hdf5_indices(self._features, feature_list)
        results = self._build_results_template(expression)

        feature_expressions = []
        if self._include_metadata:
            counts = self.get_raw_counts()
            feature_counts = []

        for feature_id in indices:
            results["features"].append(feature_id)
            slice_index = indices[feature_id]
            feature_expressions.append(list(expression[...,slice_index]))

            if self._include_metadata:
                feature_counts.append(list(counts[...,slice_index]))

        feature_zip = zip(*feature_expressions)
        samples_list = list(map(bytes.decode, self.get_samples()))
        results["expression"] = dict(zip(samples_list, (map(list, feature_zip))))

        if self._include_metadata:
            counts_zip = zip(*feature_counts)
            results["metadata"]["raw_counts"] = dict(zip(samples_list, (map(list,counts_zip))))

        return results

    def search(self, sample_id=None, feature_list=None):
        # TODO: support multiple sample_id searches?
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
                    results["features"].append(feature_id)
                    slice_index = feature_indices[feature_id]
                    feature_expressions.append(sample_expressions[slice_index])

                    if self._include_metadata:
                        feature_counts.append(sample_counts[slice_index])

                results["expression"][sample_id] = feature_expressions

                if self._include_metadata:
                    results["metadata"]["raw_counts"][sample_id] = feature_counts

            return results

        elif sample_id:
            return self._search_sample(sample_id)

        elif feature_list:
            return self._search_features(feature_list)

        return {}

    def search_threshold(self, ft_list, ft_type="min"):
        feature_list = list(zip(*ft_list))[0]
        raw_results = self._search_features(feature_list)
        return self._apply_threshold(ft_list, raw_results, ft_type)

    def _apply_threshold(self, ft_list, results, ft_type):

        if ft_type == "min":
            def ft_compare(x,y):
                return x > y
        else:
            def ft_compare(x,y):
                return x < y

        for feature, threshold in ft_list:
            feature_index = results["features"].index(feature)
            prune_list = []
            # prune samples from results based on threshold settings
            for sample in results["expression"]:
                if ft_compare(results["expression"][sample][feature_index], threshold):
                    continue
                else:
                    prune_list.append(sample)

            for sample in prune_list:
                del results["expression"][sample]
                if self._include_metadata:
                    # Update metadata if exists
                    del results["metadata"]["raw_counts"][sample]

        return results

    def _split_column(self, col_array):
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

        return results

    def json_out(self, search_results, filename):
        """
        Create a json formatted document containing query results
        TODO: temporary for POC and testing, HDF5/Loom/Parquet etc. will be necessary file outputs for large queries
        """
        output = filename
        with open(output, 'w') as outfile:
            json.dump(search_results, outfile)

        print('>>> Output to: {}'.format(output))

    def hdf5_out(self, search_results):
        """
        Create an HDF5 (RNA-API spec compliant) of search results
        TODO: this will be necessary
        """
        raise NotImplementedError

    def close(self):
        self._file.close()

# TODO: delete examples when done
# if __name__ == "__main__":
#     query_tool = ExpressionQueryTool(__TEST_DIR__, include_metadata=True)
#     feature_threshold_list = [('ENSG00000000003', 0.35), ('ENSG00000000005', 0.2), ('LRG_3', 0.35)]
#
#     results_1 = query_tool.search(feature_list=['ENSG00000000003', 'ENSG00000000005', 'LRG_3'])
#     results_2 = query_tool.search(sample_id='PATIENT_20')
#     results_3 = query_tool.search(sample_id='PATIENT_8', feature_list=['ENSG00000000003', 'LRG_3'])
#     results_4 = query_tool.search_threshold(feature_threshold_list, ft_type="max")
#     #query_tool.json_out(results_1, "expression_results.json")
