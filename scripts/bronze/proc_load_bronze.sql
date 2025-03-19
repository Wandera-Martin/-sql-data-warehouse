CREATE OR REPLACE PROCEDURE bronze.load_bronze() -- Create a procedure to load the data
LANGUAGE plpgsql -- Language of the procedure
AS $$ -- Start of the procedure
DECLARE -- Declare variables
    v_start_time TIMESTAMP; -- Variable to store the start time
    v_end_time TIMESTAMP; -- Variable to store the end time
    batch_start_time TIMESTAMP; -- Variable to store the start time of a batch
    batch_end_time TIMESTAMP; -- Variable to store the end time of a batch
BEGIN
    batch_start_time = NOW(); -- Get the current time
    RAISE NOTICE '========================================================'; 
    RAISE NOTICE 'Loading Bronze Layer'; -- Print a message
    RAISE NOTICE '========================================================';

    RAISE NOTICE '========================================================';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '========================================================';

    v_start_time = NOW(); -- Get the current time 
    -- Load the CRM tables
    RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info; -- Truncate the table before loading the data
    COPY bronze.crm_cust_info -- Bulk load data into the table(Bulk insert)
        FROM 'C:\Users\Public\Projects\-sql-data-warehouse\data\source_crm\cust_info.csv'
        WITH (
            FORMAT csv, -- Format of the file
            HEADER true, -- File has a header row
            DELIMITER ',' -- Delimiter used in the file
        );
    v_end_time = NOW(); -- Get the current time
    RAISE NOTICE 'Load Duration: %', v_end_time - v_start_time; -- Print the time taken to load the table
    RAISE NOTICE '--------------------------------------------------------';

    v_start_time = NOW(); -- Get the current time
    RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info
        FROM 'C:\Users\Public\Projects\-sql-data-warehouse\data\source_crm\prd_info.csv'
        WITH (
            FORMAT csv,
            HEADER true, 
            DELIMITER ','
        );
    v_end_time = NOW(); -- Get the current time
    RAISE NOTICE 'Load Duration: %', v_end_time - v_start_time; -- Print the time taken to load the table
    RAISE NOTICE '--------------------------------------------------------';

    RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details
        FROM 'C:\Users\Public\Projects\-sql-data-warehouse\data\source_crm\sales_details.csv'
        WITH (
            FORMAT csv, 
            HEADER true, 
            DELIMITER ','
        );
    v_end_time = NOW(); -- Get the current time
    RAISE NOTICE 'Load Duration: %', v_end_time - v_start_time; -- Print the time taken to load the table
    RAISE NOTICE '--------------------------------------------------------';

    RAISE NOTICE '========================================================';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '========================================================';

    -- Load the ERP tables
    v_start_time = NOW(); -- Get the current time
    RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12
        FROM 'C:\Users\Public\Projects\-sql-data-warehouse\data\source_erp\cust_az12.csv'
        WITH (FORMAT csv, 
        HEADER true, 
        DELIMITER ','
    );
    v_end_time = NOW(); -- Get the current time
    RAISE NOTICE 'Load Duration: %', v_end_time - v_start_time; -- Print the time taken to load the table
    RAISE NOTICE '--------------------------------------------------------';

    v_start_time = NOW(); -- Get the current time
    RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101
        FROM 'C:\Users\Public\Projects\-sql-data-warehouse\data\source_erp\loc_a101.csv'
        WITH (
            FORMAT csv, 
            HEADER true, 
            DELIMITER ','
        );
    v_end_time = NOW(); -- Get the current time
    RAISE NOTICE 'Load Duration: %', v_end_time - v_start_time; -- Print the time taken to load the table
    RAISE NOTICE '--------------------------------------------------------';

    v_start_time = NOW(); -- Get the current time
    RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2
        FROM 'C:\Users\Public\Projects\-sql-data-warehouse\data\source_erp\px_cat_g1v2.csv'
        WITH (
            FORMAT csv, 
            HEADER true, 
            DELIMITER ','
        );
    RAISE NOTICE '--------------------------------------------------------'; 
    v_end_time = NOW(); -- Get the current time
    RAISE NOTICE 'Loading Bronze Layer is Completed';
    RAISE NOTICE '  - Total Load Duration: %', v_end_time - v_start_time; -- Print the time taken to load the table
    RAISE NOTICE '--------------------------------------------------------'; 
    batch_end_time = NOW(); -- Get the current time
EXCEPTION -- Exception block to handle errors
    WHEN OTHERS THEN -- Catch all exceptions
        RAISE NOTICE 'An error occurred: %', SQLERRM;-- Print the error message
END;
$$;-- End of the procedure
