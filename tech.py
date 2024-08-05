#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import talib, pandas as pd
import sys
from os import listdir
from os.path import isfile, join

current_path = sys.path[0]


# In[ ]:


### Все доступные функции в пакете TA-lib

# print(talib.get_functions())
# print(talib.get_function_groups())


# In[ ]:


def calculator(filename):
    path = current_path + "/datasets/" + filename


    ## Overlap Studies
    BBANDS_full_upperband = pd.DataFrame()
    BBANDS_full_middleband = pd.DataFrame()
    BBANDS_full_lowerband = pd.DataFrame()
    DEMA_full = pd.DataFrame()
    EMA_full = pd.DataFrame()
    HT_TRENDLINE_full = pd.DataFrame()
    KAMA_full = pd.DataFrame()
    MA_full = pd.DataFrame()
    MAMA_full_mama = pd.DataFrame()
    MAMA_full_fama = pd.DataFrame()
    MAVP_full = pd.DataFrame()
    MIDPOINT_full = pd.DataFrame()
    MIDPRICE_full = pd.DataFrame()
    SAR_full = pd.DataFrame()
    SAREXT_full = pd.DataFrame()
    SMA_full = pd.DataFrame()
    T3_full = pd.DataFrame()
    TEMA_full = pd.DataFrame()
    TRIMA_full = pd.DataFrame()
    WMA_full = pd.DataFrame()



    ## Momentum Indicators
    ADX_full = pd.DataFrame()
    ADXR_full = pd.DataFrame()
    APO_full = pd.DataFrame()
    AROON_down_full = pd.DataFrame()
    AROON_up_full = pd.DataFrame()
    AROONOSC_full = pd.DataFrame()
    BOP_full = pd.DataFrame()
    CCI_full = pd.DataFrame()
    CMO_full = pd.DataFrame()
    DX_full = pd.DataFrame()
    MACD_real_full = pd.DataFrame()
    MACD_signal_full = pd.DataFrame()
    MACD_hist_full = pd.DataFrame()
    MACDEXT_real_full = pd.DataFrame()
    MACDEXT_signal_full = pd.DataFrame()
    MACDEXT_hist_full = pd.DataFrame()
    MACDFIX_real_full = pd.DataFrame()
    MACDFIX_signal_full = pd.DataFrame()
    MACDFIX_hist_full = pd.DataFrame()
    MFI_full = pd.DataFrame()
    MINUS_DI_full = pd.DataFrame()
    MINUS_DM_full = pd.DataFrame()
    MOM_full = pd.DataFrame()
    PLUS_DI_full = pd.DataFrame()
    PLUS_DM_full = pd.DataFrame()
    PPO_full = pd.DataFrame()
    ROC_full = pd.DataFrame()
    ROCP_full = pd.DataFrame()
    ROCR_full = pd.DataFrame()
    ROCR100_full = pd.DataFrame()
    RSI_full = pd.DataFrame()
    STOCH_full_slowk = pd.DataFrame()
    STOCH_full_slowd = pd.DataFrame()
    STOCHF_full_fastk = pd.DataFrame()
    STOCHF_full_fastd = pd.DataFrame()
    STOCHRSI_full_fastk = pd.DataFrame()
    STOCHRSI_full_fastd = pd.DataFrame()
    TRIX_full = pd.DataFrame()
    ULTOSC_full = pd.DataFrame()
    WILLR_full = pd.DataFrame()


    ## Volume Indicators
    AD_full = pd.DataFrame()
    ADOSC_full = pd.DataFrame()
    OBV_full = pd.DataFrame()


    ## Volatility Indicators
    ATR_full = pd.DataFrame()
    NATR_full = pd.DataFrame()
    TRANGE_full = pd.DataFrame()

    ## Price Transform Functions
    AVGPRICE_full = pd.DataFrame()
    MEDPRICE_full = pd.DataFrame()
    TYPPRICE_full = pd.DataFrame()
    WCLPRICE_full = pd.DataFrame()

    ## Cycle Indicators
    HT_DCPERIOD_full = pd.DataFrame()
    HT_DCPHASE_full = pd.DataFrame()
    HT_PHASOR_full_inphase = pd.DataFrame()
    HT_PHASOR_full_quadrature = pd.DataFrame()
    HT_SINE_full_sine = pd.DataFrame()
    HT_SINE_full_leadsine = pd.DataFrame()
    HT_TRENDMODE_full = pd.DataFrame()

    ## Pattern Recognition

    ## Statistic Functions
    BETA_full = pd.DataFrame()
    CORREL_full = pd.DataFrame()
    LINEARREG_full = pd.DataFrame()
    LINEARREG_ANGLE_full = pd.DataFrame()
    LINEARREG_INTERCEPT_full = pd.DataFrame()
    LINEARREG_SLOPE_full = pd.DataFrame()
    STDDEV_full = pd.DataFrame()
    TSF_full = pd.DataFrame()
    VAR_full = pd.DataFrame()
    
    
  
    ## Другие показатели    
    
    HighsLows_full = pd.DataFrame()
    BullBearPower_full = pd.DataFrame()
    

    

    if path.endswith('csv') and "~$" not in path:
        df = pd.read_csv(path)
    elif path.endswith('xlsx') and "~$" not in path:
        df = pd.read_excel(path)


    ### Проверка наличия колонки индикатора в df. Убирает в случае наличия. Необходимо для пересчёта индикаторов без перевыгрузки данных
    
    ## Overlap Studies Functions
    if 'BBANDS_upperband' in df.columns: df.drop('BBANDS_upperband', axis=1, inplace=True) #BBANDS - Bollinger Bands
    if 'BBANDS_middleband' in df.columns: df.drop('BBANDS_middleband', axis=1, inplace=True) #BBANDS - Bollinger Bands
    if 'BBANDS_lowerband' in df.columns: df.drop('BBANDS_lowerband', axis=1, inplace=True) #BBANDS - Bollinger Bands
    if 'DEMA' in df.columns: df.drop('DEMA', axis=1, inplace=True) #DEMA - Double Exponential Moving Average
    if 'EMA' in df.columns: df.drop('EMA', axis=1, inplace=True) #EMA - Exponential Moving Average
    if 'HT_TRENDLINE' in df.columns: df.drop('HT_TRENDLINE', axis=1, inplace=True) #HT_TRENDLINE - Hilbert Transform - Instantaneous Trendline
    if 'KAMA' in df.columns: df.drop('KAMA', axis=1, inplace=True) #KAMA - Kaufman Adaptive Moving Average
    if 'MA' in df.columns: df.drop('MA', axis=1, inplace=True) #MA - Moving average
    if 'MAMA_mama' in df.columns: df.drop('MAMA_mama', axis=1, inplace=True) #MAMA - MESA Adaptive Moving Average
    if 'MAMA_fama' in df.columns: df.drop('MAMA_fama', axis=1, inplace=True) #MAMA - MESA Adaptive Moving Average
    if 'MAVP' in df.columns: df.drop('MAVP', axis=1, inplace=True) #MAVP - Moving average with variable period
    if 'MIDPOINT' in df.columns: df.drop('MIDPOINT', axis=1, inplace=True) #MIDPOINT - MidPoint over period
    if 'MIDPRICE' in df.columns: df.drop('MIDPRICE', axis=1, inplace=True) #MIDPRICE - Midpoint Price over period
    if 'SAR' in df.columns: df.drop('SAR', axis=1, inplace=True) #SAR - Parabolic SAR
    if 'SAREXT' in df.columns: df.drop('SAREXT', axis=1, inplace=True) #SAREXT - Parabolic SAR - Extended
    if 'SMA' in df.columns: df.drop('SMA', axis=1, inplace=True) #SMA - Simple Moving Average
    if 'T3' in df.columns: df.drop('T3', axis=1, inplace=True) #T3 - Triple Exponential Moving Average (T3)
    if 'TEMA' in df.columns: df.drop('TEMA', axis=1, inplace=True) #TEMA - Triple Exponential Moving Average
    if 'TRIMA' in df.columns: df.drop('TRIMA', axis=1, inplace=True) #TRIMA - Triangular Moving Average
    if 'WMA' in df.columns: df.drop('WMA', axis=1, inplace=True) #WMA - Weighted Moving Average


    ## Momentum Indicator Functions
    if 'ADX' in df.columns: df.drop('ADX', axis=1, inplace=True) #ADX - Average Directional Movement Index
    if 'ADXR' in df.columns: df.drop('ADXR', axis=1, inplace=True) #ADXR - Average Directional Movement Index Rating
    if 'APO' in df.columns: df.drop('APO', axis=1, inplace=True) #APO - Absolute Price Oscillator
    if 'AROON_down' in df.columns: df.drop('AROON_down', axis=1, inplace=True) #AROON - Aroon
    if 'AROON_up' in df.columns: df.drop('AROON_up', axis=1, inplace=True) #AROON - Aroon
    if 'AROONOSC' in df.columns: df.drop('AROONOSC', axis=1, inplace=True) #AROONOSC - Aroon Oscillator
    if 'BOP' in df.columns: df.drop('BOP', axis=1, inplace=True) #BOP - Balance Of Power
    if 'CCI' in df.columns: df.drop('CCI', axis=1, inplace=True) #CCI - Commodity Channel Index
    if 'CMO' in df.columns: df.drop('CMO', axis=1, inplace=True) #CMO - Chande Momentum Oscillator
    if 'DX' in df.columns: df.drop('DX', axis=1, inplace=True) #DX - Directional Movement Index
    if 'MACD_real' in df.columns: df.drop('MACD_real', axis=1, inplace=True) #MACD - Moving Average Convergence/Divergence
    if 'MACD_signal' in df.columns: df.drop('MACD_signal', axis=1, inplace=True) #MACD - Moving Average Convergence/Divergence
    if 'MACD_hist' in df.columns: df.drop('MACD_hist', axis=1, inplace=True) #MACD - Moving Average Convergence/Divergence
    if 'MACDEXT_real' in df.columns: df.drop('MACDEXT_real', axis=1, inplace=True) #MACDEXT - MACD with controllable MA type
    if 'MACDEXT_signal' in df.columns: df.drop('MACDEXT_signal', axis=1, inplace=True) #MACDEXT - MACD with controllable MA type
    if 'MACDEXT_hist' in df.columns: df.drop('MACDEXT_hist', axis=1, inplace=True) #MACDEXT - MACD with controllable MA type
    if 'MACDFIX_real' in df.columns: df.drop('MACDFIX_real', axis=1, inplace=True) #MACDFIX - Moving Average Convergence/Divergence Fix 12/26
    if 'MACDFIX_signal' in df.columns: df.drop('MACDFIX_signal', axis=1, inplace=True) #MACDFIX - Moving Average Convergence/Divergence Fix 12/26
    if 'MACDFIX_hist' in df.columns: df.drop('MACDFIX_hist', axis=1, inplace=True) #MACDFIX - Moving Average Convergence/Divergence Fix 12/26
    if 'MFI' in df.columns: df.drop('MFI', axis=1, inplace=True) #MFI - Money Flow Index
    if 'MINUS_DI' in df.columns: df.drop('MINUS_DI', axis=1, inplace=True) #MINUS_DI - Minus Directional Indicator
    if 'MINUS_DM' in df.columns: df.drop('MINUS_DM', axis=1, inplace=True) #MINUS_DM - Minus Directional Movement
    if 'MOM' in df.columns: df.drop('MOM', axis=1, inplace=True) #MOM - Momentum
    if 'PLUS_DI' in df.columns: df.drop('PLUS_DI', axis=1, inplace=True) #PLUS_DI - Plus Directional Indicator
    if 'PLUS_DM' in df.columns: df.drop('PLUS_DM', axis=1, inplace=True) #PLUS_DM - Plus Directional Movement
    if 'PPO' in df.columns: df.drop('PPO', axis=1, inplace=True) #PPO - Percentage Price Oscillator
    if 'ROC' in df.columns: df.drop('ROC', axis=1, inplace=True) #ROC - Rate of change : ((price/prevPrice)-1)*100
    if 'ROCP' in df.columns: df.drop('ROCP', axis=1, inplace=True) #ROCP - Rate of change Percentage: (price-prevPrice)/prevPrice
    if 'ROCR' in df.columns: df.drop('ROCR', axis=1, inplace=True) #ROCR - Rate of change ratio: (price/prevPrice)
    if 'ROCR100' in df.columns: df.drop('ROCR100', axis=1, inplace=True) #ROCR100 - Rate of change ratio 100 scale: (price/prevPrice)*100
    if 'RSI14' in df.columns: df.drop('RSI14', axis=1, inplace=True) #RSI - Relative Strength Index
    if 'STOCH_slowk' in df.columns: df.drop('STOCH_slowk', axis=1, inplace=True) #STOCH - Stochastic
    if 'STOCH_slowd' in df.columns: df.drop('STOCH_slowd', axis=1, inplace=True) #STOCH - Stochastic
    if 'STOCHF_fastk' in df.columns: df.drop('STOCHF_fastk', axis=1, inplace=True) #STOCHF - Stochastic Fast
    if 'STOCHF_fastd' in df.columns: df.drop('STOCHF_fastd', axis=1, inplace=True) #STOCHF - Stochastic Fast
    if 'STOCHRSI_fastk' in df.columns: df.drop('STOCHRSI_fastk', axis=1, inplace=True) #STOCHRSI - Stochastic Relative Strength Index
    if 'STOCHRSI_fastd' in df.columns: df.drop('STOCHRSI_fastd', axis=1, inplace=True) #STOCHRSI - Stochastic Relative Strength Index
    if 'TRIX' in df.columns: df.drop('TRIX', axis=1, inplace=True) #TRIX - 1-day Rate-Of-Change (ROC) of a Triple Smooth EMA
    if 'ULTOSC' in df.columns: df.drop('ULTOSC', axis=1, inplace=True) #ULTOSC - Ultimate Oscillator
    if 'WILLR' in df.columns: df.drop('WILLR', axis=1, inplace=True) #WILLR - Williams' %R

    
    
    ## Volatility Indicator Functions
    if 'ATR' in df.columns: df.drop('ATR', axis=1, inplace=True) #ATR - Average True Range
    if 'NATR' in df.columns: df.drop('NATR', axis=1, inplace=True) #NATR - Normalized Average True Range
    if 'TRANGE' in df.columns: df.drop('TRANGE', axis=1, inplace=True) #TRANGE - True Range



    ## Volume Indicator Functions
    if 'AD' in df.columns: df.drop('AD', axis=1, inplace=True) #AD - Chaikin A/D Line
    if 'ADOSC' in df.columns: df.drop('ADOSC', axis=1, inplace=True) #ADOSC - Chaikin A/D Oscillator
    if 'OBV' in df.columns: df.drop('OBV', axis=1, inplace=True) #OBV - On Balance Volume



    ## Price Transform Functions
    if 'AVGPRICE' in df.columns: df.drop('AVGPRICE', axis=1, inplace=True) #AVGPRICE - Average Price
    if 'MEDPRICE' in df.columns: df.drop('MEDPRICE', axis=1, inplace=True) #MEDPRICE - Median Price
    if 'TYPPRICE' in df.columns: df.drop('TYPPRICE', axis=1, inplace=True) #TYPPRICE - Typical Price
    if 'WCLPRICE' in df.columns: df.drop('WCLPRICE', axis=1, inplace=True) #WCLPRICE - Weighted Close Price



    ## Cycle Indicator Functions
    if 'HT_DCPERIOD' in df.columns: df.drop('HT_DCPERIOD', axis=1, inplace=True) #HT_DCPERIOD - Hilbert Transform - Dominant Cycle Period
    if 'HT_DCPHASE' in df.columns: df.drop('HT_DCPHASE', axis=1, inplace=True) #HT_DCPHASE - Hilbert Transform - Dominant Cycle Phase
    if 'HT_PHASOR_inphase' in df.columns: df.drop('HT_PHASOR_inphase', axis=1, inplace=True) #HT_PHASOR - Hilbert Transform - Phasor Components
    if 'HT_PHASOR_quadrature' in df.columns: df.drop('HT_PHASOR_quadrature', axis=1, inplace=True) #HT_PHASOR - Hilbert Transform - Phasor Components
    if 'HT_SINE_sine' in df.columns: df.drop('HT_SINE_sine', axis=1, inplace=True) #HT_SINE - Hilbert Transform - SineWave
    if 'HT_SINE_leadsine' in df.columns: df.drop('HT_SINE_leadsine', axis=1, inplace=True) #HT_SINE - Hilbert Transform - SineWave
    if 'HT_TRENDMODE' in df.columns: df.drop('HT_TRENDMODE', axis=1, inplace=True) #HT_TRENDMODE - Hilbert Transform - Trend vs Cycle Mode



    ## Statistic Functions
    if 'BETA' in df.columns: df.drop('BETA', axis=1, inplace=True) #BETA - Beta
    if 'CORREL' in df.columns: df.drop('CORREL', axis=1, inplace=True) #CORREL - Pearson's Correlation Coefficient (r)
    if 'LINEARREG' in df.columns: df.drop('LINEARREG', axis=1, inplace=True) #LINEARREG - Linear Regression
    if 'LINEARREG_ANGLE' in df.columns: df.drop('LINEARREG_ANGLE', axis=1, inplace=True) #LINEARREG_ANGLE - Linear Regression Angle
    if 'LINEARREG_INTERCEPT' in df.columns: df.drop('LINEARREG_INTERCEPT', axis=1, inplace=True) #LINEARREG_INTERCEPT - Linear Regression Intercept
    if 'LINEARREG_SLOPE' in df.columns: df.drop('LINEARREG_SLOPE', axis=1, inplace=True) #LINEARREG_SLOPE - Linear Regression Slope
    if 'STDDEV' in df.columns: df.drop('STDDEV', axis=1, inplace=True) #STDDEV - Standard Deviation
    if 'TSF' in df.columns: df.drop('TSF', axis=1, inplace=True) #TSF - Time Series Forecast
    if 'VAR' in df.columns: df.drop('VAR', axis=1, inplace=True) #VAR - Variance


    ## Другие показатели
    if 'HighsLows' in df.columns: df.drop('HighsLows', axis=1, inplace=True)
    if 'BullBearPower' in df.columns: df.drop('BullBearPower', axis=1, inplace=True)
    

    ticker_list = df['ticker'].to_list()
    ticker_list = list(set(ticker_list))

    for i in range(0,len(ticker_list)):
        ticker = ticker_list[i]
        df_ticker = df[(df['ticker'] == ticker)]

        ## Overlap Studies Functions
        BBANDS_real_ticker_upperband, BBANDS_real_ticker_middleband, BBANDS_real_ticker_lowerband = talib.BBANDS(df_ticker['close'], timeperiod=5, nbdevup=2, nbdevdn=2, matype=0)
        if len(BBANDS_real_ticker_upperband) > 0 and len(BBANDS_real_ticker_middleband) > 0 and len(BBANDS_real_ticker_lowerband) > 0:
            BBANDS_full_upperband = pd.concat([BBANDS_full_upperband,BBANDS_real_ticker_upperband])
            BBANDS_full_middleband = pd.concat([BBANDS_full_middleband,BBANDS_real_ticker_middleband])
            BBANDS_full_lowerband = pd.concat([BBANDS_full_lowerband,BBANDS_real_ticker_lowerband])

        DEMA_real_ticker = talib.DEMA(df_ticker['close'], timeperiod=30)
        if len(DEMA_real_ticker) > 0: DEMA_full = pd.concat([DEMA_full,DEMA_real_ticker])

        EMA_real_ticker = talib.EMA(df_ticker['close'], timeperiod=30)
        if len(EMA_real_ticker) > 0: EMA_full = pd.concat([EMA_full,EMA_real_ticker])

        HT_TRENDLINE_real_ticker = talib.HT_TRENDLINE(df_ticker['close'])
        if len(HT_TRENDLINE_real_ticker) > 0: HT_TRENDLINE_full = pd.concat([HT_TRENDLINE_full,HT_TRENDLINE_real_ticker])

        KAMA_real_ticker = talib.KAMA(df_ticker['close'], timeperiod=30)
        if len(KAMA_real_ticker) > 0: KAMA_full = pd.concat([KAMA_full,KAMA_real_ticker])

        MA_real_ticker = talib.MA(df_ticker['close'], timeperiod=30, matype=0)
        if len(MA_real_ticker) > 0: MA_full = pd.concat([MA_full,MA_real_ticker])

        MAMA_real_ticker_mama, MAMA_real_ticker_fama = talib.MAMA(df_ticker['close'], fastlimit=0, slowlimit=0)
        if len(MAMA_real_ticker_mama) > 0 and len(MAMA_real_ticker_fama) > 0:
            MAMA_full_mama = pd.concat([MAMA_full_mama,MAMA_real_ticker_mama])
            MAMA_full_fama = pd.concat([MAMA_full_fama,MAMA_real_ticker_fama])

        # разобраться с периодами
        # MAVP_real_ticker = talib.MAVP(df_ticker['close'], periods, minperiod=2, maxperiod=30, matype=0)
        # if len(MAVP_real_ticker) > 0: MAVP_full = pd.concat([MAVP_full,MAVP_real_ticker])

        MIDPOINT_real_ticker = talib.MIDPOINT(df_ticker['close'], timeperiod=14)
        if len(MIDPOINT_real_ticker) > 0: MIDPOINT_full = pd.concat([MIDPOINT_full,MIDPOINT_real_ticker])

        MIDPRICE_real_ticker = talib.MIDPRICE(df_ticker['high'], df_ticker['low'], timeperiod=14)
        if len(MIDPRICE_real_ticker) > 0: MIDPRICE_full = pd.concat([MIDPRICE_full,MIDPRICE_real_ticker])

        SAR_real_ticker = talib.SAR(df_ticker['high'], df_ticker['low'], acceleration=0, maximum=0)
        if len(SAR_real_ticker) > 0: SAR_full = pd.concat([SAR_full,SAR_real_ticker])

        SAREXT_real_ticker = talib.SAREXT(df_ticker['close'], timeperiod=30)
        if len(SAREXT_real_ticker) > 0: SAREXT_full = pd.concat([SAREXT_full,SAREXT_real_ticker])

        T3_real_ticker = talib.T3(df_ticker['high'], timeperiod=5, vfactor=0)
        if len(T3_real_ticker) > 0: T3_full = pd.concat([T3_full,T3_real_ticker])

        TEMA_real_ticker = talib.TEMA(df_ticker['close'], timeperiod=30)
        if len(TEMA_real_ticker) > 0: TEMA_full = pd.concat([TEMA_full,TEMA_real_ticker])

        TRIMA_real_ticker = talib.TRIMA(df_ticker['close'], timeperiod=30)
        if len(TRIMA_real_ticker) > 0: TRIMA_full = pd.concat([TRIMA_full,TRIMA_real_ticker])

        WMA_real_ticker = talib.WMA(df_ticker['close'], timeperiod=30)
        if len(WMA_real_ticker) > 0: WMA_full = pd.concat([WMA_full,WMA_real_ticker])



        ## Momentum Indicator Functions
        ADX_real_ticker = talib.ADX(df_ticker['high'], df_ticker['low'], df_ticker['close'], timeperiod=14)
        if len(ADX_real_ticker) > 0: ADX_full = pd.concat([ADX_full,ADX_real_ticker])

        ADXR_real_ticker = talib.ADXR(df_ticker['high'], df_ticker['low'], df_ticker['close'], timeperiod=14)
        if len(ADXR_real_ticker) > 0: ADXR_full = pd.concat([ADXR_full,ADXR_real_ticker])

        APO_real_ticker = talib.APO(df_ticker['close'], fastperiod=12, slowperiod=26, matype=0)
        if len(APO_real_ticker) > 0: APO_full = pd.concat([APO_full,APO_real_ticker])

        AROON_down_ticker, AROON_up_ticker = talib.AROON(df_ticker['high'], df_ticker['low'], timeperiod=14)
        if len(AROON_down_ticker) > 0 and len(AROON_up_ticker) > 0:
            AROON_down_full = pd.concat([AROON_down_full,AROON_down_ticker])
            AROON_up_full = pd.concat([AROON_up_full,AROON_up_ticker])

        AROONOSC_real_ticker = talib.AROONOSC(df_ticker['high'], df_ticker['low'], timeperiod=14)
        if len(AROONOSC_real_ticker) > 0: AROONOSC_full = pd.concat([AROONOSC_full, AROONOSC_real_ticker])

        BOP_real_ticker = talib.BOP(df_ticker['open'], df_ticker['high'], df_ticker['low'], df_ticker['close'])
        if len(BOP_real_ticker) > 0: BOP_full = pd.concat([BOP_full,BOP_real_ticker])

        CCI_real_ticker = talib.CCI(df_ticker['high'], df_ticker['low'], df_ticker['close'], timeperiod=14)
        if len(CCI_real_ticker) > 0: CCI_full = pd.concat([CCI_full,CCI_real_ticker])

        CMO_real_ticker = talib.CMO(df_ticker['close'], timeperiod=14) # тут не уверен, в доке real указано брать
        if len(CMO_real_ticker) > 0: CMO_full = pd.concat([CMO_full,CMO_real_ticker])

        DX_real_ticker = talib.DX(df_ticker['high'], df_ticker['low'], df_ticker['close'], timeperiod=14)
        if len(DX_real_ticker) > 0: DX_full = pd.concat([DX_full,DX_real_ticker])

        MACD_real_ticker, MACD_signal_ticker, MACD_hist_ticker = talib.MACD(df_ticker['close'], fastperiod=12, slowperiod=26, signalperiod=9)
        if len(MACD_real_ticker) > 0 and len(MACD_signal_ticker) > 0 and len(MACD_hist_ticker) > 0:
            MACD_real_full = pd.concat([MACD_real_full,MACD_real_ticker])
            MACD_signal_full = pd.concat([MACD_signal_full,MACD_signal_ticker])
            MACD_hist_full = pd.concat([MACD_hist_full,MACD_hist_ticker])

        MACDEXT_real_ticker, MACDEXT_signal_ticker, MACDEXT_hist_ticker = talib.MACDEXT(df_ticker['close'], fastperiod=12, fastmatype=0, slowperiod=26, slowmatype=0, signalperiod=9, signalmatype=0)
        if len(MACDEXT_real_ticker) > 0 and len(MACDEXT_signal_ticker) > 0 and len(MACDEXT_hist_ticker) > 0:
            MACDEXT_real_full = pd.concat([MACDEXT_real_full,MACDEXT_real_ticker])
            MACDEXT_signal_full = pd.concat([MACDEXT_signal_full,MACDEXT_signal_ticker])
            MACDEXT_hist_full = pd.concat([MACDEXT_hist_full,MACDEXT_hist_ticker])

        MACDFIX_real_ticker, MACDFIX_signal_ticker, MACDFIX_hist_ticker = talib.MACDFIX(df_ticker['close'], signalperiod=9)
        if len(MACDFIX_real_ticker) > 0 and len(MACDFIX_signal_ticker) > 0 and len(MACDFIX_hist_ticker) > 0:
            MACDFIX_real_full = pd.concat([MACDFIX_real_full,MACDFIX_real_ticker])
            MACDFIX_signal_full = pd.concat([MACDFIX_signal_full,MACDFIX_signal_ticker])
            MACDFIX_hist_full = pd.concat([MACDFIX_hist_full,MACDFIX_hist_ticker])

        MFI_real_ticker = talib.MFI(df_ticker['high'], df_ticker['low'], df_ticker['close'], df_ticker['volume'], timeperiod=14)
        if len(MFI_real_ticker) > 0: MFI_full = pd.concat([MFI_full,MFI_real_ticker])

        MINUS_DI_real_ticker = talib.MINUS_DI(df_ticker['high'], df_ticker['low'], df_ticker['close'], timeperiod=14)
        if len(MINUS_DI_real_ticker) > 0: MINUS_DI_full = pd.concat([MINUS_DI_full,MINUS_DI_real_ticker])

        MINUS_DM_real_ticker = talib.MINUS_DM(df_ticker['high'], df_ticker['low'], timeperiod=14)
        if len(MINUS_DM_real_ticker) > 0: MINUS_DM_full = pd.concat([MINUS_DM_full,MINUS_DM_real_ticker])

        MOM_real_ticker = talib.MOM(df_ticker['close'], timeperiod=100)
        if len(MOM_real_ticker) > 0: MOM_full = pd.concat([MOM_full,MOM_real_ticker])

        PLUS_DI_real_ticker = talib.PLUS_DI(df_ticker['high'], df_ticker['low'], df_ticker['close'], timeperiod=14)
        if len(PLUS_DI_real_ticker) > 0: PLUS_DI_full = pd.concat([PLUS_DI_full,PLUS_DI_real_ticker])

        PLUS_DM_real_ticker = talib.PLUS_DM(df_ticker['high'], df_ticker['low'],timeperiod=14)
        if len(PLUS_DM_real_ticker) > 0: PLUS_DM_full = pd.concat([PLUS_DM_full,PLUS_DM_real_ticker])

        PPO_real_ticker = talib.PPO(df_ticker['close'], fastperiod=12, slowperiod=26, matype=0)
        if len(PPO_real_ticker) > 0: PPO_full = pd.concat([PPO_full,PPO_real_ticker])

        ROC_real_ticker = talib.ROC(df_ticker['close'], timeperiod=10)
        if len(ROC_real_ticker) > 0: ROC_full = pd.concat([ROC_full,ROC_real_ticker])

        ROCP_real_ticker = talib.ROCP(df_ticker['close'], timeperiod=10)
        if len(ROCP_real_ticker) > 0: ROCP_full = pd.concat([ROCP_full,ROCP_real_ticker])

        ROCR_real_ticker = talib.ROCR(df_ticker['close'], timeperiod=10)
        if len(ROCR_real_ticker) > 0: ROCR_full = pd.concat([ROCR_full,ROCR_real_ticker])

        ROCR100_real_ticker = talib.ROCR100(df_ticker['close'], timeperiod=10)
        if len(ROCR100_real_ticker) > 0: ROCR100_full = pd.concat([ROCR100_full,ROCR100_real_ticker])

        RSI_ticker = talib.RSI(df_ticker["close"], timeperiod=14)
        if len(RSI_ticker) > 0: RSI_full = pd.concat([RSI_full,RSI_ticker])

        STOCH_real_ticker_slowk, STOCH_real_ticker_slowd = talib.STOCH(df_ticker['high'], df_ticker['low'], df_ticker['close'], fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0)
        if len(STOCH_real_ticker_slowk) > 0 and len(STOCH_real_ticker_slowd) > 0:
            STOCH_full_slowk = pd.concat([STOCH_full_slowk,STOCH_real_ticker_slowk])
            STOCH_full_slowd = pd.concat([STOCH_full_slowd,STOCH_real_ticker_slowk])

        STOCHF_real_ticker_fastk, STOCHF_real_ticker_fastd = talib.STOCHF(df_ticker['high'], df_ticker['low'], df_ticker['close'], fastk_period=5, fastd_period=3, fastd_matype=0)
        if len(STOCHF_real_ticker_fastk) > 0 and len(STOCHF_real_ticker_fastd) > 0:
            STOCHF_full_fastk = pd.concat([STOCHF_full_fastk,STOCHF_real_ticker_fastk])
            STOCHF_full_fastd = pd.concat([STOCHF_full_fastd,STOCHF_real_ticker_fastd])
        
        STOCHRSI_ticker_fastk, STOCHRSI_ticker_fastd = talib.STOCHRSI(df_ticker['close'], timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=0)
        if len(STOCHRSI_ticker_fastk) > 0 and len(STOCHRSI_ticker_fastd) > 0:
            STOCHRSI_full_fastk = pd.concat([STOCHRSI_full_fastk,STOCHRSI_ticker_fastk])
            STOCHRSI_full_fastd = pd.concat([STOCHRSI_full_fastd,STOCHRSI_ticker_fastd])

        TRIX_real_ticker = talib.TRIX(df_ticker['close'], timeperiod=30)
        if len(TRIX_real_ticker) > 0: _full = pd.concat([TRIX_full, TRIX_real_ticker])
  
        ULTOSC_real_ticker = talib.ULTOSC(df_ticker['high'], df_ticker['low'], df_ticker['close'], timeperiod1=7, timeperiod2=14, timeperiod3=28)
        if len(ULTOSC_real_ticker) > 0: ULTOSC_full = pd.concat([ULTOSC_full,ULTOSC_real_ticker])
  
        WILLR_real_ticker = talib.WILLR(df_ticker['high'], df_ticker['low'], df_ticker['close'], timeperiod=14)
        if len(WILLR_real_ticker) > 0: WILLR_full = pd.concat([WILLR_full,WILLR_real_ticker])
        


        ## Volatility Indicator Functions
        ATR_real_ticker = talib.ATR(df_ticker['high'], df_ticker['low'], df_ticker['close'], timeperiod=14)
        if len(ATR_real_ticker) > 0: ATR_full = pd.concat([ATR_full,ATR_real_ticker])

        NATR_real_ticker = talib.NATR(df_ticker['high'], df_ticker['low'], df_ticker['close'], timeperiod=14)
        if len(NATR_real_ticker) > 0: NATR_full = pd.concat([NATR_full,NATR_real_ticker])

        TRANGE_real_ticker = talib.TRANGE(df_ticker['high'], df_ticker['low'], df_ticker['close'])
        if len(TRANGE_real_ticker) > 0: TRANGE_full = pd.concat([TRANGE_full,TRANGE_real_ticker])




        ## Volume Indicator Functions
        AD_real_ticker = talib.AD(df_ticker['high'], df_ticker['low'], df_ticker['close'], df_ticker['volume'])
        if len(AD_real_ticker) > 0: AD_full = pd.concat([AD_full,AD_real_ticker])

        ADOSC_real_ticker = talib.ADOSC(df_ticker['high'], df_ticker['low'], df_ticker['close'],df_ticker['volume'], fastperiod=3, slowperiod=10)
        if len(ADOSC_real_ticker) > 0: ADOSC_full = pd.concat([ADOSC_full,ADOSC_real_ticker])

        OBV_real_ticker = talib.OBV(df_ticker['close'], df_ticker['volume'])
        if len(OBV_real_ticker) > 0: OBV_full = pd.concat([OBV_full,OBV_real_ticker])



        ## Price Transform Functions
        AVGPRICE_real_ticker = talib.AVGPRICE(df_ticker['open'], df_ticker['high'], df_ticker['low'], df_ticker['close'])
        if len(AVGPRICE_real_ticker) > 0: AVGPRICE_full = pd.concat([AVGPRICE_full,AVGPRICE_real_ticker])

        MEDPRICE_real_ticker = talib.MEDPRICE(df_ticker['high'], df_ticker['low'])
        if len(MEDPRICE_real_ticker) > 0: MEDPRICE_full = pd.concat([MEDPRICE_full,MEDPRICE_real_ticker])

        TYPPRICE_real_ticker = talib.TYPPRICE(df_ticker['high'], df_ticker['low'], df_ticker['close'])
        if len(TYPPRICE_real_ticker) > 0: TYPPRICE_full = pd.concat([TYPPRICE_full,TYPPRICE_real_ticker])

        WCLPRICE_real_ticker = talib.WCLPRICE(df_ticker['high'], df_ticker['low'], df_ticker['close'])
        if len(WCLPRICE_real_ticker) > 0: WCLPRICE_full = pd.concat([WCLPRICE_full,WCLPRICE_real_ticker])

        ## Cycle Indicator Functions
        HT_DCPERIOD_real_ticker = talib.HT_DCPERIOD(df_ticker['close'])
        if len(HT_DCPERIOD_real_ticker) > 0: HT_DCPERIOD_full = pd.concat([HT_DCPERIOD_full,HT_DCPERIOD_real_ticker])

        HT_DCPHASE_real_ticker = talib.HT_DCPHASE(df_ticker['close'])
        if len(HT_DCPHASE_real_ticker) > 0: HT_DCPHASE_full = pd.concat([HT_DCPHASE_full,HT_DCPHASE_real_ticker])

        HT_PHASOR_real_ticker_inphase, HT_PHASOR_real_ticker_quadrature = talib.HT_PHASOR(df_ticker['close'])
        if len(HT_PHASOR_real_ticker_inphase) > 0 and len(HT_PHASOR_real_ticker_quadrature) > 0:
            HT_PHASOR_full_inphase = pd.concat([HT_PHASOR_full_inphase,HT_PHASOR_real_ticker_inphase])
            HT_PHASOR_full_quadrature = pd.concat([HT_PHASOR_full_quadrature,HT_PHASOR_real_ticker_quadrature])

        HT_SINE_real_ticker_sine, HT_SINE_real_ticker_leadsine = talib.HT_SINE(df_ticker['close'])
        if len(HT_SINE_real_ticker_sine) > 0 and len(HT_SINE_real_ticker_leadsine) > 0 :
            HT_SINE_full_sine = pd.concat([HT_SINE_full_sine,HT_SINE_real_ticker_sine])
            HT_SINE_full_leadsine = pd.concat([HT_SINE_full_leadsine,HT_SINE_real_ticker_leadsine])

        HT_TRENDMODE_real_ticker = talib.HT_TRENDMODE(df_ticker['close'])
        if len(HT_TRENDMODE_real_ticker) > 0: HT_TRENDMODE_full = pd.concat([HT_TRENDMODE_full,HT_TRENDMODE_real_ticker])




        ## Statistic Functions

        # для реализации нужно сперва протащить данные по индексам, так как он используется в значении real10 https://github.com/TA-Lib/ta-lib/blob/master/src/ta_func/ta_BETA.c#L207
        # BETA_real_ticker = talib.BETA(real0, df_ticker['close'], timeperiod=5)
        # if len(BETA_real_ticker) > 0: BETA_full = pd.concat([BETA_full, BETA_real_ticker])

        # для реализации нужно сперва протащить данные по индексам, так как он используется в значении real10
        # CORREL_real_ticker = talib.CORREL(real10 df_ticker['close'], timeperiod=30)
        # if len(CORREL_real_ticker) > 0: CORREL_full = pd.concat([CORREL_full,CORREL_real_ticker])


        LINEARREG_real_ticker = talib.LINEARREG(df_ticker['close'], timeperiod=14)
        if len(LINEARREG_real_ticker) > 0: LINEARREG_full = pd.concat([LINEARREG_full,LINEARREG_real_ticker])

        LINEARREG_ANGLE_real_ticker = talib.LINEARREG_ANGLE(df_ticker['close'], timeperiod=14)
        if len(LINEARREG_ANGLE_real_ticker) > 0: LINEARREG_ANGLE_full = pd.concat([LINEARREG_ANGLE_full,LINEARREG_ANGLE_real_ticker])

        LINEARREG_INTERCEPT_real_ticker = talib.LINEARREG_INTERCEPT(df_ticker['close'], timeperiod=14)
        if len(LINEARREG_INTERCEPT_real_ticker) > 0: LINEARREG_INTERCEPT_full = pd.concat([LINEARREG_INTERCEPT_full,LINEARREG_INTERCEPT_real_ticker])

        LINEARREG_SLOPE_real_ticker = talib.LINEARREG_SLOPE(df_ticker['close'], timeperiod=14)
        if len(LINEARREG_SLOPE_real_ticker) > 0: LINEARREG_SLOPE_full = pd.concat([LINEARREG_SLOPE_full,LINEARREG_SLOPE_real_ticker])

        STDDEV_real_ticker = talib.STDDEV(df_ticker['close'], timeperiod=5, nbdev=1)
        if len(STDDEV_real_ticker) > 0: STDDEV_full = pd.concat([STDDEV_full,STDDEV_real_ticker])

        TSF_real_ticker = talib.TSF(df_ticker['close'], timeperiod=14)
        if len(TSF_real_ticker) > 0: TSF_full = pd.concat([TSF_full,TSF_real_ticker])

        VAR_real_ticker = talib.VAR(df_ticker['close'], timeperiod=5, nbdev=1)
        if len(VAR_real_ticker) > 0: VAR_full = pd.concat([VAR_full,VAR_real_ticker])



        ## Другие показатели

        # HighsLows_real_ticker = talib.
        # if len(HighsLows_real_ticker) > 0: HighsLows_full = pd.concat([HighsLows_full,HighsLows_real_ticker])
        
               
        # BullBearPower_real_ticker = talib.
        # if len(BullBearPower_real_ticker) > 0: BullBearPower_full = pd.concat([BullBearPower_full,BullBearPower_real_ticker])


    ### Overlap Studies Functions
    BBANDS_full_upperband = BBANDS_full_upperband.rename(columns= {0: 'BBANDS_upperband'})
    df = df.join(BBANDS_full_upperband)

    BBANDS_full_middleband = BBANDS_full_middleband.rename(columns= {0: 'BBANDS_middleband'})
    df = df.join(BBANDS_full_middleband)

    BBANDS_full_lowerband = BBANDS_full_lowerband.rename(columns= {0: 'BBANDS_lowerband'})
    df = df.join(BBANDS_full_lowerband)

    DEMA_full = DEMA_full.rename(columns= {0: 'DEMA'})
    df = df.join(DEMA_full)

    EMA_full = EMA_full.rename(columns= {0: 'EMA'})
    df = df.join(EMA_full)

    HT_TRENDLINE_full = HT_TRENDLINE_full.rename(columns= {0: 'HT_TRENDLINE'})
    df = df.join(HT_TRENDLINE_full)

    KAMA_full = KAMA_full.rename(columns= {0: 'KAMA'})
    df = df.join(KAMA_full)

    MA_full = MA_full.rename(columns= {0: 'MA'})
    df = df.join(MA_full)

    MAMA_full_mama = MAMA_full_mama.rename(columns= {0: 'MAMA_mama'})
    df = df.join(MAMA_full_mama)

    MAMA_full_fama = MAMA_full_fama.rename(columns= {0: 'MAMA_fama'})
    df = df.join(MAMA_full_fama)

    MAVP_full = MAVP_full.rename(columns= {0: 'MAVP'})
    df = df.join(MAVP_full)

    MAVP_full = MAVP_full.rename(columns= {0: 'MAVP'})
    df = df.join(MAVP_full)

    MIDPOINT_full = MIDPOINT_full.rename(columns= {0: 'MIDPOINT'})
    df = df.join(MIDPOINT_full)

    MIDPRICE_full = MIDPRICE_full.rename(columns= {0: 'MIDPRICE'})
    df = df.join(MIDPRICE_full)

    SAR_full = SAR_full.rename(columns= {0: 'SAR'})
    df = df.join(SAR_full)

    SAREXT_full = SAREXT_full.rename(columns= {0: 'SAREXT'})
    df = df.join(SAREXT_full)

    SMA_full = SMA_full.rename(columns= {0: 'SMA'})
    df = df.join(SMA_full)

    T3_full = T3_full.rename(columns= {0: 'T3'})
    df = df.join(T3_full)

    TEMA_full = TEMA_full.rename(columns= {0: 'TEMA'})
    df = df.join(TEMA_full)

    TEMA_full = TEMA_full.rename(columns= {0: 'TEMA'})
    df = df.join(TEMA_full)

    TRIMA_full = TRIMA_full.rename(columns= {0: 'TRIMA'})
    df = df.join(TRIMA_full)

    WMA_full = WMA_full.rename(columns= {0: 'WMA'})
    df = df.join(WMA_full)



    ### Momentum Indicator Functions
    ADX_full = ADX_full.rename(columns= {0: 'ADX'})
    df = df.join(ADX_full)

    ADXR_full = ADXR_full.rename(columns= {0: 'ADXR'})
    df = df.join(ADXR_full)

    APO_full = APO_full.rename(columns= {0: 'APO'})
    df = df.join(APO_full)

    AROON_down_full = AROON_down_full.rename(columns= {0: 'AROON_down'})
    df = df.join(AROON_down_full)
    
    AROON_up_full = AROON_up_full.rename(columns= {0: 'AROON_up'})
    df = df.join(AROON_up_full)

    AROONOSC_full = AROONOSC_full.rename(columns= {0: 'AROONOSC'})
    df = df.join(AROONOSC_full)

    BOP_full = BOP_full.rename(columns= {0: 'BOP'})
    df = df.join(BOP_full)

    CCI_full = CCI_full.rename(columns= {0: 'CCI'})
    df = df.join(CCI_full)

    CMO_full = CMO_full.rename(columns= {0: 'CMO'})
    df = df.join(CMO_full)

    DX_full = DX_full.rename(columns= {0: 'DX'})
    df = df.join(DX_full)

    MACD_real_full = MACD_real_full.rename(columns= {0: 'MACD_real'})
    df = df.join(MACD_real_full)

    MACD_signal_full = MACD_signal_full.rename(columns= {0: 'MACD_signal'})
    df = df.join(MACD_signal_full)

    MACD_hist_full = MACD_hist_full.rename(columns= {0: 'MACD_hist'})
    df = df.join(MACD_hist_full)

    MACDEXT_real_full = MACDEXT_real_full.rename(columns= {0: 'MACDEXT_real'})
    df = df.join(MACDEXT_real_full)

    MACDEXT_signal_full = MACDEXT_signal_full.rename(columns= {0: 'MACDEXT_signal'})
    df = df.join(MACDEXT_signal_full)

    MACDEXT_hist_full = MACDEXT_hist_full.rename(columns= {0: 'MACDEXT_hist'})
    df = df.join(MACDEXT_hist_full)

    MACDFIX_real_full = MACDFIX_real_full.rename(columns= {0: 'MACDFIX_real'})
    df = df.join(MACDFIX_real_full)

    MACDFIX_signal_full = MACDFIX_signal_full.rename(columns= {0: 'MACDFIX_signal'})
    df = df.join(MACDFIX_signal_full)

    MACDFIX_hist_full = MACDFIX_hist_full.rename(columns= {0: 'MACDFIX_hist'})
    df = df.join(MACDFIX_hist_full)

    MFI_full = MFI_full.rename(columns= {0: 'MFI'})
    df = df.join(MFI_full)

    MINUS_DI_full = MINUS_DI_full.rename(columns= {0: 'MINUS_DI'})
    df = df.join(MINUS_DI_full)

    MINUS_DM_full = MINUS_DM_full.rename(columns= {0: 'MINUS_DM'})
    df = df.join(MINUS_DM_full)

    MOM_full = MOM_full.rename(columns= {0: 'MOM'})
    df = df.join(MOM_full)

    PLUS_DI_full = PLUS_DI_full.rename(columns= {0: 'PLUS_DI'})
    df = df.join(PLUS_DI_full)

    PLUS_DM_full = PLUS_DM_full.rename(columns= {0: 'PLUS_DM'})
    df = df.join(PLUS_DM_full)

    PPO_full = PPO_full.rename(columns= {0: 'PPO'})
    df = df.join(PPO_full)

    ROC_full = ROC_full.rename(columns= {0: 'ROC'})
    df = df.join(ROC_full)

    ROCP_full = ROCP_full.rename(columns= {0: 'ROCP'})
    df = df.join(ROCP_full)

    ROCR_full = ROCR_full.rename(columns= {0: 'ROCR'})
    df = df.join(ROCR_full)

    ROCR100_full = ROCR100_full.rename(columns= {0: 'ROCR100'})
    df = df.join(ROCR100_full)

    RSI_full = RSI_full.rename(columns= {0: 'RSI14'})
    df = df.join(RSI_full)

    STOCH_full_slowk = STOCH_full_slowk.rename(columns= {0: 'STOCH_slowk'})
    df = df.join(STOCH_full_slowk)

    STOCH_full_slowd = STOCH_full_slowd.rename(columns= {0: 'STOCH_slowd'})
    df = df.join(STOCH_full_slowd)

    STOCHF_full_fastk = STOCHF_full_fastk.rename(columns= {0: 'STOCHF_fastk'})
    df = df.join(STOCHF_full_fastk)

    STOCHF_full_fastd = STOCHF_full_fastd.rename(columns= {0: 'STOCHF_fastd'})
    df = df.join(STOCHF_full_fastd)

    STOCHRSI_full_fastk = STOCHRSI_full_fastk.rename(columns= {0: 'STOCHRSI_fastk'})
    df = df.join(STOCHRSI_full_fastk)

    STOCHRSI_full_fastd = STOCHRSI_full_fastd.rename(columns= {0: 'STOCHRSI_fastd'})
    df = df.join(STOCHRSI_full_fastd)

    TRIX_full = TRIX_full.rename(columns= {0: 'TRIX'})
    df = df.join(TRIX_full)

    ULTOSC_full = ULTOSC_full.rename(columns= {0: 'ULTOSC'})
    df = df.join(ULTOSC_full)

    WILLR_full = WILLR_full.rename(columns= {0: 'WILLR'})
    df = df.join(WILLR_full)



    ## Volatility Indicator Functions
    ATR_full = ATR_full.rename(columns= {0: 'ATR'})
    df = df.join(ATR_full)

    NATR_full = NATR_full.rename(columns= {0: 'NATR'})
    df = df.join(NATR_full)

    TRANGE_full = TRANGE_full.rename(columns= {0: 'TRANGE'})
    df = df.join(TRANGE_full)



    ## Volume Indicator Functions
    AD_full = AD_full.rename(columns= {0: 'AD'})
    df = df.join(AD_full)

    ADOSC_full = ADOSC_full.rename(columns= {0: 'ADOSC'})
    df = df.join(ADOSC_full)

    OBV_full = OBV_full.rename(columns= {0: 'OBV'})
    df = df.join(OBV_full)



    ##Price Transform Functions
    AVGPRICE_full = AVGPRICE_full.rename(columns= {0: 'AVGPRICE'})
    df = df.join(AVGPRICE_full)

    MEDPRICE_full = MEDPRICE_full.rename(columns= {0: 'MEDPRICE'})
    df = df.join(MEDPRICE_full)

    TYPPRICE_full = TYPPRICE_full.rename(columns= {0: 'TYPPRICE'})
    df = df.join(TYPPRICE_full)

    WCLPRICE_full = WCLPRICE_full.rename(columns= {0: 'WCLPRICE'})
    df = df.join(WCLPRICE_full)



    ## Cycle Indicator Functions
    HT_DCPERIOD_full = HT_DCPERIOD_full.rename(columns= {0: 'HT_DCPERIOD'})
    df = df.join(HT_DCPERIOD_full)

    HT_DCPHASE_full = HT_DCPHASE_full.rename(columns= {0: 'HT_DCPHASE'})
    df = df.join(HT_DCPHASE_full)

    HT_PHASOR_full_inphase = HT_PHASOR_full_inphase.rename(columns= {0: 'HT_PHASOR_inphase'})
    df = df.join(HT_PHASOR_full_inphase)

    HT_PHASOR_full_quadrature = HT_PHASOR_full_quadrature.rename(columns= {0: 'HT_PHASOR_quadrature'})
    df = df.join(HT_PHASOR_full_quadrature)


    HT_SINE_full_sine = HT_SINE_full_sine.rename(columns= {0: 'HT_SINE_sine'})
    df = df.join(HT_SINE_full_sine)


    HT_SINE_full_leadsine = HT_SINE_full_leadsine.rename(columns= {0: 'HT_SINE_leadsine'})
    df = df.join(HT_SINE_full_leadsine)

    HT_TRENDMODE_full = HT_TRENDMODE_full.rename(columns= {0: 'HT_TRENDMODE'})
    df = df.join(HT_TRENDMODE_full)



    ## Statistic Functions
    BETA_full = BETA_full.rename(columns= {0: 'BETA'})
    df = df.join(BETA_full)

    CORREL_full = CORREL_full.rename(columns= {0: 'CORREL'})
    df = df.join(CORREL_full)

    LINEARREG_full = LINEARREG_full.rename(columns= {0: 'LINEARREG'})
    df = df.join(LINEARREG_full)

    LINEARREG_ANGLE_full = LINEARREG_ANGLE_full.rename(columns= {0: 'LINEARREG_ANGLE'})
    df = df.join(LINEARREG_ANGLE_full)

    LINEARREG_INTERCEPT_full = LINEARREG_INTERCEPT_full.rename(columns= {0: 'LINEARREG_INTERCEPT'})
    df = df.join(LINEARREG_INTERCEPT_full)

    LINEARREG_SLOPE_full = LINEARREG_SLOPE_full.rename(columns= {0: 'LINEARREG_SLOPE'})
    df = df.join(LINEARREG_SLOPE_full)

    STDDEV_full = STDDEV_full.rename(columns= {0: 'STDDEV'})
    df = df.join(STDDEV_full)

    TSF_full = TSF_full.rename(columns= {0: 'TSF'})
    df = df.join(TSF_full)

    VAR_full = VAR_full.rename(columns= {0: 'VAR'})
    df = df.join(VAR_full)



    ## Другие показатели         
    HighsLows_full = HighsLows_full.rename(columns= {0: 'HighsLows'})
    df = df.join(HighsLows_full)
             
    BullBearPower_full = BullBearPower_full.rename(columns= {0: 'BullBearPower'})
    df = df.join(BullBearPower_full)

# запись файла в его исходный формат
    if path.endswith('csv'):
        if len(df) > 0: df.to_csv(path,index = False)
    elif path.endswith('xlsx'):
        if len(df) > 0: df.to_excel(path,index = False)


# In[ ]:


### Ниже блоки для проверки отдельно функции

# filename = "30years_data_1h_interval.csv"
# path =  current_path + "/datasets/" + filename
# df = pd.read_csv(path)
# ticker_list = df['ticker'].to_list()
# ticker_list = list(set(ticker_list))

# for i in range(0,len(ticker_list)):
#     ticker = ticker_list[i]
#     df_ticker = df[(df['ticker'] == ticker)]


# In[ ]:


# real = talib.WILLR(df_ticker['high'], df_ticker['low'], df_ticker['close'], timeperiod=14)
# real


# In[ ]:


def main():
    #getting datasets
    datasets_list = [f for f in listdir(current_path + '/datasets') if isfile(join(current_path + '/datasets', f))]

    # datasets_list = [x for x in datasets_list if not x.endswith('.csv')]

    datasets_list = list(set(datasets_list))

    for file in datasets_list:
        if (file.endswith('csv') and "~$" not in file) or (file.endswith('xlsx') and "~$" not in file):
            try:
                calculator(file)
            except Exception as e:
                print (e)
                print(file)
                print('------------------------')


# In[ ]:


if __name__ == "__main__": 
    main()

