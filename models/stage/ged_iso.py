import pandas as pd
import country_converter as coco

def model(dbt, fal):
    ged = dbt.source('raw', 'ged')
    ged_countries = ged.country.unique()
    ged = pd.DataFrame(
        {"country": ged_countries,
        "iso3c": coco.convert(ged_countries, to="ISO3"),
        "iso2c": coco.convert(ged_countries, to="ISO2")})
    #ged['iso3c'] = coco.convert(ged.country.unique(), to="ISO3")
    #ged['iso3c'] = coco.convert(ged.country, to="ISO3")
    return ged