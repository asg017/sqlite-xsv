#!/bin/bash
python3 -c "import pandas as pd; df = pd.read_csv('../_data/totals.csv'); print(len(df.index))";