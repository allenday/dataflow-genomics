import pandas as pd
import logging
import matplotlib.pyplot as plt

logging.getLogger().setLevel(logging.INFO)

existence_df = pd.read_csv("existence_rice.csv", delimiter=",")
logging.info(existence_df['size2'].sum())
existence_df['size2'].plot(kind="hist", bins=30)
plt.show()







