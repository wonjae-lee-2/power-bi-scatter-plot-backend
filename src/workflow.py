from azure.storage.blob import BlobClient
import itertools
from msal import SerializableTokenCache, PublicClientApplication
import pandas as pd
from prefect import flow, task
from prefect_dask import DaskTaskRunner
import requests
from sklearn import linear_model

n_workers = 15
scope_powerbi = ['https://analysis.windows.net/powerbi/api/.default']
scope_sharepoint = ['https://graph.microsoft.com/.default']
query = '''
    EVALUATE
    VAR FIRST_YEAR = 2017
    VAR LAST_YEAR = 2022
    VAR TREE_NAME = "COSTCENTRES 2022-01-01"
    RETURN
        SUMMARIZECOLUMNS (
            'Date'[Fiscal Year],
            'Cost Centre'[Region 2020],
            'Cost Centre'[RegOff/Country],
            FILTER ( 'Filter Operation', 'Filter Operation'[Filter_Operation] = "Y" ),
            FILTER ( 'Date', FIRST_YEAR <= 'Date'[Fiscal Year] && 'Date'[Fiscal Year] <= LAST_YEAR ),
            FILTER (
                'Situation Reporting',
                'Situation Reporting'[Fund] IN { "-", "PILLAR1", "PILLAR2", "PILLAR3", "PILLAR4" }
            ),
            FILTER ( 'Cost Centre', 'Cost Centre'[Tree Name] = TREE_NAME ),
            "Budget_OL", [Budget_OL],
            "Budget_OL_ABOD", [Budget_OL_ABOD],
            "Budget_OL_OPS", [Budget_OL_OPS],
            "Budget_OL_Pillar_1", [Budget_OL_Pillar_1],
            "Budget_OL_Pillar_2", [Budget_OL_Pillar_2],
            "Budget_OL_Pillar_3", [Budget_OL_Pillar_3],
            "Budget_OL_Pillar_4", [Budget_OL_Pillar_4],
            "Budget_OL_STAFF", [Budget_OL_STAFF],
            "Budget_OP", [Budget_OP],
            "Budget_OP_ABOD", [Budget_OP_ABOD],
            "Budget_OP_OPS", [Budget_OP_OPS],
            "Budget_OP_Pillar_1", [Budget_OP_Pillar_1],
            "Budget_OP_Pillar_2", [Budget_OP_Pillar_2],
            "Budget_OP_Pillar_3", [Budget_OP_Pillar_3],
            "Budget_OP_Pillar_4", [Budget_OP_Pillar_4],
            "Budget_OP_STAFF", [Budget_OP_STAFF],
            "Budget_WOL_ABOD", [Budget_WOL_ABOD],
            "Budget_WOL_ABOD_Fix", [Budget_WOL_ABOD_Fix],
            "Budget_WOL_ABOD_Var", [Budget_WOL_ABOD_Var],
            "Budget_WOL_CBI", [Budget_WOL_CBI],
            "Budget_WOL_Direct", [Budget_WOL_Direct],
            "Budget_WOL_OPS", [Budget_WOL_OPS],
            "Budget_WOL_Partner", [Budget_WOL_Partner],
            "Expenditure_KK", [Expenditure_KK],
            "Income_Contribution", [Income_Contribution],
            "Income_Funds_Available", [Income_Funds_Available],
            "Inv_Distribution_Last_12_Months", [Inv_Distribution_Last_12_Months],
            "Inv_Turnover_Last_12_Months", [Inv_Turnover_Last_12_Months],
            "Inv_Value_Yearend", [Inv_Value_Yearend],
            "Partner_Audit_Qualified", [Partner_Audit_Qualified],
            "Partner_Num_Partner", [Partner_Num_Partner],
            "Partner_Num_PPA", [Partner_Num_PPA],
            "POC_Assisted", [POC_Assisted],
            "POC_Assisted_Pillar_1", [POC_Assisted_Pillar_1],
            "POC_Assisted_Pillar_2", [POC_Assisted_Pillar_2],
            "POC_Assisted_Pillar_3", [POC_Assisted_Pillar_3],
            "POC_Assisted_Pillar_4", [POC_Assisted_Pillar_4],
            "POC_Total", [POC_Total],
            "POC_Total_Pillar_1", [POC_Total_Pillar_1],
            "POC_Total_Pillar_2", [POC_Total_Pillar_2],
            "POC_Total_Pillar_3", [POC_Total_Pillar_3],
            "POC_Total_Pillar_4", [POC_Total_Pillar_4],
            "Ratio_Budget_OL_ABOD_vs_Budget_OL", [Ratio_Budget_OL_ABOD_vs_Budget_OL],
            "Ratio_Budget_OL_OPS_vs_Budget_OL", [Ratio_Budget_OL_OPS_vs_Budget_OL],
            "Ratio_Budget_OL_STAFF_vs_Budget_OL", [Ratio_Budget_OL_STAFF_vs_Budget_OL],
            "Ratio_Budget_OL_STAFF_vs_Staff_Count", [Ratio_Budget_OL_STAFF_vs_Staff_Count],
            "Ratio_Budget_OL_vs_Budget_OP", [Ratio_Budget_OL_vs_Budget_OP],
            "Ratio_Budget_OL_vs_POC_Assisted", [Ratio_Budget_OL_vs_POC_Assisted],
            "Ratio_Budget_OL_vs_POC_Total", [Ratio_Budget_OL_vs_POC_Total],
            "Ratio_Budget_OL_vs_Staff_Count", [Ratio_Budget_OL_vs_Staff_Count],
            "Ratio_Budget_OP_ABOD_vs_Budget_OP", [Ratio_Budget_OP_ABOD_vs_Budget_OP],
            "Ratio_Budget_OP_OPS_vs_Budget_OP", [Ratio_Budget_OP_OPS_vs_Budget_OP],
            "Ratio_Budget_OP_STAFF_vs_Budget_OP", [Ratio_Budget_OP_STAFF_vs_Budget_OP],
            "Ratio_Budget_OP_vs_POC_Assisted", [Ratio_Budget_OP_vs_POC_Assisted],
            "Ratio_Budget_OP_vs_POC_Total", [Ratio_Budget_OP_vs_POC_Total],
            "Ratio_Budget_WOL_ABOD_Fix_vs_Budget_WOL_ABOD", [Ratio_Budget_WOL_ABOD_Fix_vs_Budget_WOL_ABOD],
            "Ratio_Budget_WOL_ABOD_Var_vs_Budget_WOL_ABOD", [Ratio_Budget_WOL_ABOD_Var_vs_Budget_WOL_ABOD],
            "Ratio_Budget_WOL_CBI_vs_Budget_WOL_OPS", [Ratio_Budget_WOL_CBI_vs_Budget_WOL_OPS],
            "Ratio_Budget_WOL_Direct_vs_Budget_WOL_OPS", [Ratio_Budget_WOL_Direct_vs_Budget_WOL_OPS],
            "Ratio_Budget_WOL_Partner_vs_Partner_Num_Partner", [Ratio_Budget_WOL_Partner_vs_Partner_Num_Partner],
            "Ratio_Budget_WOL_Partner_vs_Partner_Num_PPA", [Ratio_Budget_WOL_Partner_vs_Partner_Num_PPA],
            "Ratio_Budget_WOL_Partner_vs_Budget_WOL_OPS", [Ratio_Budget_WOL_Partner_vs_Budget_WOL_OPS],
            "Ratio_Expenditure_KK_vs_Budget_OL", [Ratio_Expenditure_KK_vs_Budget_OL],
            "Ratio_Income_Funds_Available_vs_Budget_OL", [Ratio_Income_Funds_Available_vs_Budget_OL],
            "Ratio_Income_Funds_Available_vs_Budget_OP", [Ratio_Income_Funds_Available_vs_Budget_OP],
            "Ratio_Staff_Count_Admin_Finance_vs_Staff_Count", [Ratio_Staff_Count_Admin_Finance_vs_Staff_Count],
            "Ratio_Staff_Count_External_Relations_vs_Staff_Count", [Ratio_Staff_Count_External_Relations_vs_Staff_Count],
            "Ratio_Staff_Count_Operational_Delivery_vs_Staff_Count", [Ratio_Staff_Count_Operational_Delivery_vs_Staff_Count],
            "Staff_Count", [Staff_Count],
            "Staff_Count_Admin_Finance", [Staff_Count_Admin_Finance],
            "Staff_Count_External_Relations", [Staff_Count_External_Relations],
            "Staff_Count_Operational_Delivery", [Staff_Count_Operational_Delivery]
        )
'''
reg = linear_model.LinearRegression()


@flow
def workflow():
    token = acquire_token(scope_powerbi)
    response = query_dataset(token, query)
    df = save_data(response, 'data_input.pickle')
    df_operation = get_category_operation(df, 'data_operation.pickle')
    df_region = get_category_region(df, 'data_region.pickle')
    df_global = get_category_global(df, 'data_global.pickle')
    concat_data([df_operation, df_region, df_global], 'data_scatter_plot.csv')
    upload_to_azure('data_scatter_plot.csv')


@task
def acquire_token(scope):
    AZURE_CLI_CLIENT_ID = '04b07795-8ddb-461a-bbee-02f9e1bf7b46'
    if scope == scope_powerbi:
        cache_file = '/root/secrets/token_powerbi'
    elif scope == scope_sharepoint:
        cache_file = '/root/secrets/token_sharepoint'
    try:
        cache = SerializableTokenCache()
        with open(cache_file, 'r') as f:
            cache.deserialize(f.read())
        app = PublicClientApplication(AZURE_CLI_CLIENT_ID, token_cache=cache)
        account = app.get_accounts()[0]
        token = app.acquire_token_silent(scope, account)
        return token['access_token']
    except FileNotFoundError:
        cache = SerializableTokenCache()
        app = PublicClientApplication(AZURE_CLI_CLIENT_ID, token_cache=cache)
        flow = app.initiate_device_flow(scope)
        print(flow['message'])
        token = app.acquire_token_by_device_flow(flow)
        with open(cache_file, 'w') as f:
            f.write(cache.serialize())
        return token['access_token']


@task
def query_dataset(token, query):
    group_id = '57137b5f-f02b-4187-8708-3ec502f4ce4b'
    dataset_id = '67a78923-9fa2-4e51-b8d9-4635ec80b668'
    url = f'https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets/{dataset_id}/executeQueries'
    headers = {'Authorization': f'Bearer {token}'}
    payload = {'queries': [{'query': query}]}
    r = requests.post(url, headers=headers, json=payload)
    return r.json()


@task
def save_data(response, filename):
    df = (
        pd.DataFrame.from_records(response['results'][0]['tables'][0]['rows'])
        .rename(
            columns={
                'Date[Fiscal Year]': '[Fiscal Year]',
                'Cost Centre[Region 2020]': '[Region]',
                'Cost Centre[RegOff/Country]': '[Operation]'
            }
        )
        .rename(lambda x: x[1:-1], axis='columns')
    )
    measures = df.columns[3:]
    df1 = (
        df
        .loc[df['Fiscal Year'] == 2017]
        .fillna({x: 0 for x in measures if x[0:5] not in ['Inv_D', 'Inv_T', 'POC_A', 'POC_T', 'Ratio', 'Staff']})
    )
    df2 = (
        df
        .loc[df['Fiscal Year'] == 2018]
        .fillna({x: 0 for x in measures if x[0:5] not in ['Inv_D', 'Inv_T', 'Ratio', 'Staff']})
    )
    df3 = (
        df
        .loc[df['Fiscal Year'] > 2018]
        .fillna({x: 0 for x in measures if x[0:5] not in ['Inv_T', 'Ratio']})
    )
    df_result = pd.concat([df1, df2, df3])
    df_result.to_pickle(filename)
    return df_result


@flow(task_runner=DaskTaskRunner(cluster_kwargs={"n_workers": n_workers, "threads_per_worker": 1}))
def get_category_operation(df, filename):
    combinations_year = list(
        itertools.combinations_with_replacement(
            df['Fiscal Year'].sort_values().unique(), 2)
    )
    combinations_measure = list(
        itertools.product(df.columns[3:], repeat=2)
    )
    futures = []
    for (year_1, year_2) in combinations_year:
        if year_1 == year_2:
            future = get_category_operation_one_year.submit(
                df, year_1, combinations_measure
            )
            futures.append(future)
        else:
            future = get_category_operation_two_years.submit(
                df, year_1, year_2, combinations_measure
            )
            futures.append(future)
    df_result = pd.concat([x.result() for x in futures])
    df_result.to_pickle(filename)
    return df_result


@task
def get_category_operation_one_year(df, year, combinations_measure):
    df_list = []
    for (measure_1, measure_2) in combinations_measure:
        df1 = (
            df
            .loc[df['Fiscal Year'] == year]
            .assign(
                year1=year,
                year2=year,
                measure1=measure_1,
                measure2=measure_2,
                category='operation',
                x=lambda df: df[measure_1],
                y=lambda df: df[measure_2]
            )
            .rename(columns={'Region': 'region', 'Operation': 'operation'})
            .loc[:, ['year1', 'year2', 'measure1', 'measure2', 'category', 'region', 'operation', 'x', 'y']]
            .dropna()
        )
        if df1.empty:
            continue
        reg.fit(df1[['x']], df1['y'])
        df2 = (
            df1
            .assign(
                yhat=lambda df: reg.predict(df[['x']]),
                resid=lambda df: df.y - df.yhat,
                resid1=lambda df: ((df.resid > 0.000001) | (df.resid < -0.000001)) * df.resid,
                rmse=lambda df: df.resid1.std(ddof=0),
                upper=lambda df: df.yhat + df.rmse,
                upper2=lambda df: df.yhat + (df.rmse * 2),
                lower=lambda df: df.yhat - df.rmse,
                lower2=lambda df: df.yhat - (df.rmse * 2),
                z=lambda df: df.resid1 / df.rmse if measure_1 != measure_2 else pd.NA,
                zabs=lambda df: df.z.abs()
            )
            .loc[:, ['year1', 'year2', 'measure1', 'measure2', 'category', 'region', 'operation', 'x', 'y', 'yhat', 'upper', 'upper2', 'lower', 'lower2', 'z', 'zabs']]
        )
        df3 = pd.DataFrame({
            'year1': [year],
            'year2': [year],
            'measure1': [measure_1],
            'measure2': [measure_2],
            'category': ['correlation'],
            'correlation': [df2.x.corr(df2.y)]
        })
        df_list.append(pd.concat([df2, df3]))
    return pd.concat(df_list)


@task
def get_category_operation_two_years(df, year_1, year_2, combinations_measure):
    df_list = []
    for (measure_1, measure_2) in combinations_measure:
        df1 = (
            df
            .loc[df['Fiscal Year'].isin([year_1, year_2])]
            .assign(
                x_tmp=lambda df: df[measure_1],
                y_tmp=lambda df: df[measure_2]
            )
            .loc[:, ['Region', 'Operation', 'Fiscal Year', 'x_tmp', 'y_tmp']]
            .pivot(
                index=['Region', 'Operation'],
                columns='Fiscal Year',
                values=['x_tmp', 'y_tmp']
            )
            .assign(
                year1=year_1,
                year2=year_2,
                measure1=measure_1,
                measure2=measure_2,
                category='operation'
            )
            .assign(
                x=lambda df: df['x_tmp'][year_2] - df['x_tmp'][year_1],
                y=lambda df: df['y_tmp'][year_2] - df['y_tmp'][year_1]
            )
            .dropna()
        )
        if df1.empty:
            continue
        reg.fit(df1[['x']], df1['y'])
        df2 = (
            df1
            .assign(
                yhat=lambda df: reg.predict(df[['x']]),
                resid=lambda df: df.y - df.yhat,
                resid1=lambda df: ((df.resid > 0.000001) | (df.resid < -0.000001)) * df.resid,
                rmse=lambda df: df.resid1.std(ddof=0),
                upper=lambda df: df.yhat + df.rmse,
                upper2=lambda df: df.yhat + (df.rmse * 2),
                lower=lambda df: df.yhat - df.rmse,
                lower2=lambda df: df.yhat - (df.rmse * 2),
                z=lambda df: df.resid1 / df.rmse if measure_1 != measure_2 else pd.NA,
                zabs=lambda df: df.z.abs()
            )
            .reset_index(names=['region', 'operation'])
            .droplevel('Fiscal Year', axis=1)
            .loc[:, ['year1', 'year2', 'measure1', 'measure2', 'category', 'region', 'operation', 'x', 'y', 'yhat', 'upper', 'upper2', 'lower', 'lower2', 'z', 'zabs']]
        )
        df3 = pd.DataFrame({
            'year1': [year_1],
            'year2': [year_2],
            'measure1': [measure_1],
            'measure2': [measure_2],
            'category': ['correlation'],
            'correlation': [df2.x.corr(df2.y)]
        })
        df_list.append(pd.concat([df2, df3]))
    return pd.concat(df_list)


@flow(task_runner=DaskTaskRunner(cluster_kwargs={"n_workers": n_workers, "threads_per_worker": 1}))
def get_category_region(df, filename):
    years = df['Fiscal Year'].sort_values().unique()
    combinations_year = list(
        itertools.combinations(years, 2)
    )
    combinations_measure = list(
        itertools.product(df.columns[3:], repeat=2)
    )
    futures_one_year = []
    for year in years:
        future = get_category_region_one_year.submit(
            df, year, combinations_measure
        )
        futures_one_year.append(future)
    df_one_year = pd.concat([x.result() for x in futures_one_year])
    futures_two_years = []
    for (year_1, year_2) in combinations_year:
        future = get_category_region_two_years.submit(
            df_one_year, year_1, year_2, combinations_measure
        )
        futures_two_years.append(future)
    df_two_years = pd.concat([x.result() for x in futures_two_years])
    df_result = pd.concat([df_one_year, df_two_years])
    df_result.to_pickle(filename)
    return df_result


@task
def get_category_region_one_year(df, year, combinations_measure):
    df_list = []
    for (measure_1, measure_2) in combinations_measure:
        df1 = (
            df
            .loc[df['Fiscal Year'] == year]
            .rename(columns={'Region': 'region'})
        )
        if measure_1[0:5] == 'Ratio':
            measure_1_splits = measure_1[6:].split('_vs_')
            df1_x = (
                df1
                .assign(
                    x_numerator=lambda df: df[measure_1_splits[0]],
                    x_denominator=lambda df: df[measure_1_splits[1]]
                )
                .loc[:, ['region', 'x_numerator', 'x_denominator']]
                .groupby('region')
                .sum(min_count=1)
                .reset_index()
            )
        else:
            df1_x = (
                df1
                .assign(x_numerator=lambda df: df[measure_1])
                .loc[:, ['region', 'x_numerator']]
                .groupby('region')
                .sum(min_count=1)
                .assign(x_denominator=1)
                .reset_index()
            )
        if measure_2[0:5] == 'Ratio':
            measure_2_splits = measure_2[6:].split('_vs_')
            df1_y = (
                df1
                .assign(
                    y_numerator=lambda df: df[measure_2_splits[0]],
                    y_denominator=lambda df: df[measure_2_splits[1]]
                )
                .loc[:, ['region', 'y_numerator', 'y_denominator']]
                .groupby('region')
                .sum(min_count=1)
                .reset_index()
            )
        else:
            df1_y = (
                df1
                .assign(y_numerator=lambda df: df[measure_2])
                .loc[:, ['region', 'y_numerator']]
                .groupby('region')
                .sum(min_count=1)
                .assign(y_denominator=1)
                .reset_index()
            )
        df2 = (
            df1_x
            .merge(df1_y, on='region')
            .dropna()
            .loc[lambda df: (df.x_denominator != 0) & (df.y_denominator != 0)]
        )
        if df2.empty:
            continue
        df3 = (
            df2
            .assign(
                x=lambda df: df.x_numerator / df.x_denominator,
                y=lambda df: df.y_numerator / df.y_denominator)
            .assign(
                year1=year,
                year2=year,
                measure1=measure_1,
                measure2=measure_2,
                category='region'
            )
            .loc[:, ['year1', 'year2', 'measure1', 'measure2', 'category', 'region', 'x', 'y']]
        )
        df_list.append(df3)
    return pd.concat(df_list)


@task
def get_category_region_two_years(df, year_1, year_2, combinations_measure):
    df_list = []
    for (measure_1, measure_2) in combinations_measure:
        df1 = df.loc[(df.year1.isin([year_1, year_2])) & (df.measure1 == measure_1) & (df.measure2 == measure_2), ['year1', 'region', 'x', 'y']]
        if df1.empty:
            continue
        elif df1[df1.year1 == year_1].empty:
            continue
        elif df1[df1.year1 == year_2].empty:
            continue
        else:
            df2 = (
                df1
                .rename(columns={'x': 'x_tmp', 'y': 'y_tmp'})
                .pivot(
                    index='region',
                    columns='year1',
                    values=['x_tmp', 'y_tmp']
                )
                .assign(
                    x=lambda df: df['x_tmp'][year_2] - df['x_tmp'][year_1],
                    y=lambda df: df['y_tmp'][year_2] - df['y_tmp'][year_1]
                )
                .assign(
                    year1=year_1,
                    year2=year_2,
                    measure1=measure_1,
                    measure2=measure_2,
                    category='region'
                )
                .reset_index()
                .droplevel('year1', axis=1)
                .loc[:, ['year1', 'year2', 'measure1', 'measure2', 'category', 'region', 'x', 'y']]
                .dropna()
            )
            df_list.append(df2)
    return pd.concat(df_list)


@flow(task_runner=DaskTaskRunner(cluster_kwargs={"n_workers": n_workers, "threads_per_worker": 1}))
def get_category_global(df, filename):
    years = df['Fiscal Year'].sort_values().unique()
    combinations_year = list(
        itertools.combinations(years, 2)
    )
    combinations_measure = list(
        itertools.product(df.columns[3:], repeat=2)
    )
    futures_one_year = []
    for year in years:
        future = get_category_global_one_year.submit(
            df, year, combinations_measure
        )
        futures_one_year.append(future)
    df_one_year = pd.concat([x.result() for x in futures_one_year])
    futures_two_years = []
    for (year_1, year_2) in combinations_year:
        future = get_category_global_two_years.submit(
            df_one_year, year_1, year_2, combinations_measure
        )
        futures_two_years.append(future)
    df_two_years = pd.concat([x.result() for x in futures_two_years])
    df_result = pd.concat([df_one_year, df_two_years])
    df_result.to_pickle(filename)
    return df_result


@task
def get_category_global_one_year(df, year, combinations_measure):
    df_list = []
    for (measure_1, measure_2) in combinations_measure:
        df1 = df.loc[df['Fiscal Year'] == year]
        if measure_1[0:5] == 'Ratio':
            measure_1_splits = measure_1[6:].split('_vs_')
            s_x = (
                df1
                .assign(
                    x_numerator=lambda df: df[measure_1_splits[0]],
                    x_denominator=lambda df: df[measure_1_splits[1]]
                )
                .loc[:, ['x_numerator', 'x_denominator']]
                .sum(min_count=1)
            )
            x_numerator = s_x['x_numerator']
            x_denominator = s_x['x_denominator']
        else:
            s_x = (
                df1
                .assign(x_numerator=lambda df: df[measure_1])
                .loc[:, ['x_numerator']]
                .sum(min_count=1)
            )
            x_numerator = s_x['x_numerator']
            x_denominator = 1
             
        if measure_2[0:5] == 'Ratio':
            measure_2_splits = measure_2[6:].split('_vs_')
            s_y = (
                df1
                .assign(
                    y_numerator=lambda df: df[measure_2_splits[0]],
                    y_denominator=lambda df: df[measure_2_splits[1]]
                )
                .loc[:, ['y_numerator', 'y_denominator']]
                .sum(min_count=1)
            )
            y_numerator = s_y['y_numerator']
            y_denominator = s_y['y_denominator']
        else:
            s_y = (
                df1
                .assign(y_numerator=lambda df: df[measure_2])
                .loc[:, ['y_numerator']]
                .sum(min_count=1)
            )
            y_numerator = s_y['y_numerator']
            y_denominator = 1
        df2 = (
            pd.DataFrame({
                'x_numerator': [x_numerator],
                'x_denominator': [x_denominator],
                'y_numerator': [y_numerator],
                'y_denominator': [y_denominator]
            })
            .dropna()
            .loc[lambda df: (df.x_denominator != 0) & (df.y_denominator != 0)]
        )
        if df2.empty:
            continue
        df3 = (
            df2
            .assign(
                x=lambda df: df.x_numerator / df.x_denominator,
                y=lambda df: df.y_numerator / df.y_denominator)
            .assign(
                year1=year,
                year2=year,
                measure1=measure_1,
                measure2=measure_2,
                category='global'
            )
            .loc[:, ['year1', 'year2', 'measure1', 'measure2', 'category', 'x', 'y']]
            .dropna()
        )
        df_list.append(df3)
    return pd.concat(df_list)


@task
def get_category_global_two_years(df, year_1, year_2, combinations_measure):
    df_list = []
    for (measure_1, measure_2) in combinations_measure:       
        df1 = df.loc[(df.year1.isin([year_1, year_2])) & (df.measure1 == measure_1) & (df.measure2 == measure_2), ['year1', 'x', 'y']]
        if df1.empty:
            continue
        elif df1[df1.year1 == year_1].empty:
            continue
        elif df1[df1.year1 == year_2].empty:
            continue
        else:    
            df2 = (
                df1
                .rename(columns={'x': 'x_tmp', 'y': 'y_tmp'})
                .pivot(
                    columns='year1',
                    values=['x_tmp', 'y_tmp']
                )
                .assign(
                    x=lambda df: df['x_tmp'][year_2] - df['x_tmp'][year_1],
                    y=lambda df: df['y_tmp'][year_2] - df['y_tmp'][year_1]
                )
                .assign(
                    year1=year_1,
                    year2=year_2,
                    measure1=measure_1,
                    measure2=measure_2,
                    category='global'
                )
                .reset_index()
                .droplevel('year1', axis=1)
                .loc[:, ['year1', 'year2', 'measure1', 'measure2', 'category', 'x', 'y']]
                .dropna()
            )
            df_list.append(df2)
    return pd.concat(df_list)


@task
def concat_data(dataframes, filename):
    df = pd.concat(dataframes)
    df.to_csv(filename, index=False)


@task
def upload_to_azure(filename):
    with open('/root/workspace/secrets/azure_storage_connection_string', 'r') as f:
        connection_string = f.read()
    client = BlobClient.from_connection_string(conn_str=connection_string, container_name='powerbi', blob_name=filename)
    with open(filename, 'rb') as data:
        client.upload_blob(data, overwrite=True)


@task
def upload_to_sharepoint(token, filename):
    group_id = 'cffa4e4a-30db-419a-9e0c-37fdf7b8ebaa'
    url = f'https://graph.microsoft.com/v1.0/groups/{group_id}/drive/root:/BI Team/03. Operational Reports/Operation Analysis/data/{filename}:/content'
    headers = {'Authorization': f'Bearer {token}'}
    with open(filename, 'r') as f:
        payload = f.read()
    r = requests.put(url, headers=headers, data=payload)
    return r.json()


if __name__ == '__main__':
    with open('/root/secrets/slack_webhook_url', 'r') as f:
        slack_webhook_url = f.read()
    requests.post(slack_webhook_url, json={'text': 'Flow run Started.'})
    state = workflow(return_state=True)
    requests.post(slack_webhook_url, json={'text': f'Flow run {state}.'})
    requests.post(slack_webhook_url, json={'text': 'https://app.prefect.cloud/'})
