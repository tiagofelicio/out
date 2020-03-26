create or replace package metadata.dhub authid current_user is

    procedure dh_country_codes;

    procedure dh_covid_19;

    procedure dh_currency_codes;

    procedure dh_us_euro_foreign_exchange_rate;

end dhub;
/