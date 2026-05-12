# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.2
#   kernelspec:
#     display_name: base
#     language: python
#     name: python3
# ---

# %% [markdown]
# Проверка сколько акций было и сколько получилось

# %%
import datetime, pandas as pd, requests, csv, sys, time, os, json
current_path = sys.path[0]

# %%
df_moex = pd.read_excel(("{}/datasets/ticker_lists/moex_full.xlsx").format(current_path))

# %%
df_moex_stocks = df_moex[(df_moex['SUPERTYPE'] == "Акции")|(df_moex['SUPERTYPE'] == "Депозитарные расписки")|(df_moex['SUPERTYPE'] == 'Инвестиционные паи')|(df_moex['SUPERTYPE'] == 'Ипотечные сертификаты участия')]
df_moex_stocks.reset_index(drop=True, inplace=True)

# %%
df_moex_bonds = df_moex[(df_moex['SUPERTYPE'] == "Облигации")|(df_moex['SUPERTYPE'] == "Еврооблигации")]
df_moex_bonds.reset_index(drop=True, inplace=True)

# %%
print(len(df_moex))
print(len(df_moex_stocks))
print(len(df_moex_bonds))
print(len(df_moex_stocks) + len(df_moex_bonds))
