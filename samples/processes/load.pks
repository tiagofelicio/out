create or replace package metadata.load authid current_user is

    procedure data_hub;

    procedure data_warehouse;

end load;
/