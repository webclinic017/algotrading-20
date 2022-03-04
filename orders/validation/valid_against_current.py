"""Order validation against current positions."""
# %% codecell

import pandas as pd
import numpy as np

try:
    from scripts.dev.tdma.tdma_api.td_api import TD_API
    from scripts.dev.multiuse.help_class import getDate
except ModuleNotFoundError:
    from tdma.tdma_api.td_api import TD_API
    from scripts.dev.multiuse.help_class import getDate
# %% codecell


class OrderValidationCurrent():
    """Order validation against current positions."""

    def __init__(self, p_string, **kwargs):
        self.params = p_string.split('_')
        self.tm = self._get_account_info(self, **kwargs)
        self.df_pos = self._get_pos_df(self, self.tm, **kwargs)
        conds = self._get_conditions(self, self.df_pos, **kwargs)
        self.result = self._val_x_accept(self, conds, self.df_pos, **kwargs)
        self.result = self._no_more_dayts(self, self.tm, **kwargs)

    @classmethod
    def _unpack_params(cls, self, p_string, **kwargs):
        """Unpack parameters into dict values."""
        pvals = p_string.split('_')
        # These have already all been validated in previous function
        params = ({'action': pvals[0], 'assetType': pvals[2],
                   'quantity': pvals[3], 'symbol': pvals[4]})
        return params

    @classmethod
    def _get_account_info(cls, self, **kwargs):
        """Get account info."""
        tm = TD_API(api_val='get_accounts')
        return tm

    @classmethod
    def _get_pos_df(cls, self, tm, **kwargs):
        """Turn api call into dataframe of positions."""
        s_trans = tm.df.T
        posits = s_trans.loc['sA.positions']
        if not posits.empty:
            posits = posits[0]

        df_pos = pd.json_normalize(posits)
        df_pos['addedToday'] = np.where(
            (df_pos['previousSessionLongQuantity'] <
             df_pos['longQuantity']), True, False
        )

        return df_pos

    @classmethod
    def _get_conditions(cls, self, df_pos, **kwargs):
        """Create a list of conditions."""
        params = self.params  # Redifining for readability

        cond_list = []
        if params['action'] in ('close'):  # There should be a position open
            sell_qcond = (df_pos['longQuantity'] == int(params['quantity']))
            cond_list.append(sell_qcond)
        elif params['action'] in ('reduce'):  # Should be a position open
            sell_qcond = (df_pos['longQuantity'] >= int(params['quantity']))
            cond_list.append(sell_qcond)
        elif params['action'] in ('open'):  # No position open
            buy_qcond = (params['symbol'] not in df_pos['instrument.symbol'])
            cond_list.append(buy_qcond)

        # There should be a row titled equity
        if params['assetType'] in ('stock'):
            aT_cond = (df_pos['instrument.assetType'] == 'EQUITY')
            cond_list.append(aT_cond)

        return cond_list

    @classmethod
    def _val_x_accept(cls, self, conds, df_pos, **kwargs):
        """Validate against dataframe, acceptable order values."""
        result = True

        # Iterate through conditions and drop if any of them false
        for c in conds:
            df_c = df_pos[c].copy()
            if not df_c.empty:
                continue
            else:
                print(f"{str(c.name)} failed")
                result = False
                break

        return result

    @classmethod
    def _no_more_dayts(cls, self, tm, **kwargs):
        """Validate to make sure there either are day trades left,
           or that this won't be a day trade."""
        result = True
        assetType = self.params['assetType']
        exp_dt = False  # Have to assume that this will be some exp date
        # These two can't be equal if no more dts
        dt = getDate.query('mkt_open')

        s_trans = tm.df.T
        day_trades = s_trans.loc['sA.roundTrips'].values[0]

        # Have to assume this will be an option
        if assetType not in ('stock'):
            if day_trades == 3:  # If no more day trades available
                if dt == exp_dt:
                    result = False
        # If no more day trades available
        return result

# %% codecell
