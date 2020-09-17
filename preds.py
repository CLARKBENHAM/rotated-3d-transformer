import numpy as np
import pandas as pd

def baseline_preds():
    all_data = load_struct('all_data')
    all_data.index = all_data.index.get_level_values('date')

