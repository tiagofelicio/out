create or replace package body metadata.load is

    procedure data_hub is
    begin
        dhub.dh_country_codes;
        dhub.dh_covid_19;
        dhub.dh_currency_codes;
        dhub.dh_us_euro_foreign_exchange_rate;
    end data_hub;

    procedure data_warehouse is
    begin
        dwh.regions;
        dwh.countries;
        dwh.currencies;
        dwh.exchange_rates;
        dwh.covid_19_history;
        out.utilities.plsql(q'[
            update metadata.parameters set
                value = to_char(to_date(value, 'yyyymmdd') + 1, 'yyyymmdd')
            where code = 'reference_date'
        ]');
    end data_warehouse;

end load;
/