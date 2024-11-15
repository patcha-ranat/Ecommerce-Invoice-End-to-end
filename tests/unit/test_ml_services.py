# PYTHONPATH="code/models" pytest/unit/test_ml_services.py

import pytest
import pandas as pd
from ml_services import CustomerSegmentationService, ClusterInterpretationService


@pytest.mark.parametrize(
    "test_input, expected",
    [
        ([100, 80, 60, 45, 35, 30, 28, 27, 26, 25], 7),
        ([200, 170, 143, 121, 104, 92, 80, 69, 58, 47], 7),
        ([664529.5198954025, 466028.2616076853, 356288.1337344104, 295577.86117329565, 266924.61340523657, 249437.99099700275, 242715.77065293532, 139137.65738338124, 124541.91543760864, 116520.76974287428], 8), # unique elbow
        ([100, 60, 35, 25, 20, 18, 17, 16.5, 16, 15.8], 6), # elbow
        ([100, 80, 65, 55, 47, 40, 35, 32, 30, 28], 8),
        ([100, 50, 30, 25, 15, 14, 13.5, 13, 12.8, 12.6], 5), # elbow
        ([100, 40, 20, 18, 17, 16.5, 16.2, 16, 15.9, 15.8], 3), # elbow
        ([100, 90, 80, 70, 60, 50, 40, 30, 22, 15], 8),
        ([100, 85, 70, 55, 40, 35, 30, 28, 27.5, 27], 8),
        ([100, 50, 30, 28, 27, 26.5, 26, 25.8, 25.7, 25.6], 3), # elbow
        ([100, 40, 20, 19.5, 19.2, 19, 18.8, 18.7, 18.6, 18.5], 3), # elbow
        ([100, 90, 80, 70, 60, 50, 40, 30, 20, 10], 10), # linear
    ]
)
class TestCustomerSegmentationService:
    def test_find_optimal_k(self, test_input, expected):
        test_instance = CustomerSegmentationService(pd.DataFrame())
        assert test_instance.find_optimal_k(test_input) == expected
