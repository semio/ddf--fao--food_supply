# -*- coding: utf-8 -*-

import os
import pandas as pd

from ddf_utils.str import to_concept_id
from ddf_utils.index import get_datapackage


# config paths
source_file = '../source/FoodBalanceSheets_E_All_Data_(Norm).zip'
out_path = '../../'


if __name__ == '__main__':
    data = pd.read_csv(source_file, encoding='latin-1')
    data['concept_name'] = data['Item'] + ': ' + data['Element']

    # entities
    country = data[['Country Code', 'Country']].drop_duplicates().copy()
    country.columns = ['country', 'name']
    country.to_csv(os.path.join(out_path, 'ddf--entities--country.csv'), index=False)

    item = data[['Item Code', 'Item']].drop_duplicates().copy()
    item.columns = ['item', 'name']
    item.to_csv(os.path.join(out_path, 'ddf--entities--item.csv'), index=False)

    # concepts
    concs = data['Element'].unique()
    cdf = pd.DataFrame([], columns=['concept', 'name', 'concept_type'])
    cdf['name'] = ['Name', 'Country', 'Item', 'Year', *concs]
    cdf['concept'] = cdf['name'].map(to_concept_id)
    cdf.concept_type = 'measure'

    cdf.loc[0, 'concept_type'] = 'string'
    cdf.loc[1, 'concept_type'] = 'entity_domain'
    cdf.loc[2, 'concept_type'] = 'entity_domain'
    cdf.loc[3, 'concept_type'] = 'time'

    cdf.to_csv(os.path.join(out_path, 'ddf--concepts.csv'), index=False)

    # datapoints
    data_ = data[['Country Code', 'Item Code', 'Element', 'Year Code', 'Value']]
    gs = data_.groupby('Element').groups

    for k, idx in gs.items():
        cid = to_concept_id(k)
        df = data_.ix[idx].copy()
        df = df.drop('Element', axis=1)
        df.columns = ['country', 'item', 'year', cid]

        path = os.path.join(
            out_path, 'ddf--datapoints--{}--by--country--item--year.csv').format(cid)
        df.to_csv(path, index=False)

    get_datapackage(out_path, use_existing=True, to_disk=True)

    print('Done.')
