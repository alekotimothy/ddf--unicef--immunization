# -*- coding: utf-8 -*-

import pandas as pd
from ddf_utils.str import to_concept_id

source = '../source/wuenic2017rev_web_update.xlsx'


def process_dp(df_):
    df = df_.copy()
    df['country'] = df['iso3'].str.lower()
    df['vaccine'] = df['vaccine'].str.lower()

    df = df.drop(['unicef_region', 'iso3'], axis=1)

    df = df.set_index(['country', 'vaccine'])
    df = df.stack().reset_index()
    df.columns = ['country', 'vaccine', 'year', 'immunization_coverage']

    return df


def main():
    print('reading readme from source file...')
    readme = pd.read_excel(source)  # first tab is readme
    tabs_readme = readme.iloc[11:, 0].to_list()[:-4]  # dropping lines which are not about indicators info
    tabs = list(map(lambda x: x.split(' = ')[0], tabs_readme))
    tabs[-2] = 'ROTAC'  # fix because it's not same as tab name
    # the names of vaccine, we will save this information to entities
    vacc_names = list(map(lambda x: x.split(' = ')[1].split(' who received ')[1], tabs_readme))

    # countries datapoints
    res_df = []

    for t in tabs:
        print("reading datapoints from {} tab of source file...".format(t))
        data = pd.read_excel(source, sheet_name=t)
        res_df.append(process_dp(data))

    df_final = pd.concat(res_df, ignore_index=True, keys=['country', 'year', 'vaccine'])
    df_final = df_final.sort_values(by=['country', 'vaccine', 'year'])
    df_final.to_csv('../../ddf--datapoints--immunization_coverage--by--country--vaccine--year.csv', index=False)

    # global and regional datapoints
    data_reg = pd.read_excel(source, sheet_name='global_regional')
    df_gbl = data_reg[data_reg.region == 'Global'].copy()
    df_regs = data_reg[~(data_reg.region == 'Global')].copy()

    df_gbl_ = df_gbl.drop('group', axis=1)
    df_gbl_['region'] = 'global'
    df_gbl_.columns = ['global', 'vaccine', 'year', 'immunization_coverage']

    (df_gbl_.sort_values(by=['global', 'vaccine', 'year'])
     .to_csv('../../ddf--datapoints--immunization_coverage--by--global--vaccine--year.csv', index=False))

    df_reg_ = df_regs.drop('group', axis=1)
    df_reg_['region'] = df_reg_['region'].map(to_concept_id)
    df_reg_.columns = ['region', 'vaccine', 'year', 'immunization_coverage']
    df_reg_.sort_values(by=['region', 'vaccine', 'year']).to_csv(
        '../../ddf--datapoints--immunization_coverage--by--region--vaccine--year.csv', index=False)

    # entities: country
    cty_df = []

    for t in tabs:
        data = pd.read_excel(source, sheet_name=t)
        cty_df.append(data[['iso3', 'country']])
    country = pd.concat(cty_df, ignore_index=True, sort=False).drop_duplicates()
    country.columns = ['country', 'name']
    country['country'] = country['country'].str.lower()

    country['is--country'] = 'TRUE'
    country.to_csv('../../ddf--entities--geo--country.csv', index=False)

    # entities: global
    gbl = df_gbl[['region']].drop_duplicates()
    gbl.columns = ['global']
    gbl['name'] = 'Global'
    gbl['global'] = 'global'
    gbl['is--global'] = 'TRUE'

    gbl.to_csv('../../ddf--entities--geo--global.csv', index=False)

    # entities: region
    df_reg = data_reg[['region', 'vaccine', 'year', 'coverage']].copy()
    reg = df_reg[['region']].drop_duplicates()
    reg.columns = ['name']
    reg['region'] = reg['name'].map(to_concept_id)
    reg['is--region'] = 'TRUE'
    reg[['region', 'name', 'is--region']].to_csv('../../ddf--entities--geo--region.csv', index=False)

    # entities: vaccine
    vacc = pd.DataFrame(tabs)
    vacc.columns = ['name']
    vacc['vaccine'] = vacc['name'].str.lower()
    vacc['name'] = vacc_names
    vacc[['vaccine', 'name']].to_csv('../../ddf--entities--vaccine.csv', index=False)

    # concepts
    cdf1 = pd.DataFrame([
        ['immunization_coverage', 'Percentage of live births who recived vaccine', 'measure']
    ], columns=['concept', 'name', 'concept_type'])

    cdf2 = pd.DataFrame([
        ['name', 'Name', 'string'],
        ['year', 'Year', 'time'],
        ['domain', 'Domain', 'string'],
    ], columns=['concept', 'name', 'concept_type'])

    cdf3 = pd.DataFrame([
        ['geo', 'Geo', 'entity_domain', ''],
        ['country', 'Country', 'entity_set', 'geo'],
        ['region', 'Region', 'entity_set', 'geo'],
        ['global', 'Global', 'entity_set', 'geo'],
        ['vaccine', 'Vaccine', 'entity_domain', '']
    ], columns=['concept', 'name', 'concept_type', 'domain'])

    cdf = pd.concat([cdf1, cdf2, cdf3], ignore_index=True, sort=False)
    cdf.to_csv('../../ddf--concepts.csv', index=False)

    print("Done.")


if __name__ == '__main__':
    main()
