# Unused code for creating categorical indexes
df_sr_bins = pd.DataFrame(
                    pd.qcut(df_aapl_sr['fHigh_round'], 1000)
                ).rename(
                    columns={'fHigh_round': 'bins'}
                ).sort_values(
                    by='bins', ascending=True
                ).reset_index().drop(columns='index')