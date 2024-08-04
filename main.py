import case
import tests

# run tests first:
tests.outlier_test.test_outlier_thresholds()

# If they pass continue to run the actual code
case.pre_processing.main()
case.model_training.main()
