create or replace package metadata.dwh authid current_user is

    procedure countries;

    procedure covid_19_history;

    procedure currencies;

    procedure exchange_rates;

    procedure regions;

end dwh;
/