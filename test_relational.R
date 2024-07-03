rm(list = ls())

# set a working directory
folder <- "C:/Users/epl238/Desktop/data_test"
setwd(folder)
require(arrow)
require(duckdb)
require(DBI)
require(glue)

# Customers table
Customers <- data.frame(
    customer_id = 1:25,
    customer_name = c("John Doe", "Jane Smith", "Mike Johnson", "Emily Brown", "Michael Wilson",
                      "Emma Davis", "David Moore", "Sophia Garcia", "Andrew Martinez", "Olivia Lopez",
                      "James Taylor", "Ella Hernandez", "Logan Young", "Victoria Allen", "Daniel Scott",
                      "Grace Lewis", "Isaac Walker", "Ava King", "Mason Hill", "Chloe Green",
                      "Benjamin Baker", "Zoe Adams", "William Nelson", "Sarah Lee", "Jason Clark"),
    customer_email = c("john.doe@example.com", "jane.smith@example.com", "mike.johnson@example.com",
                       "emily.brown@example.com", "michael.wilson@example.com", "emma.davis@example.com",
                       "david.moore@example.com", "sophia.garcia@example.com", "andrew.martinez@example.com",
                       "olivia.lopez@example.com", "james.taylor@example.com", "ella.hernandez@example.com",
                       "logan.young@example.com", "victoria.allen@example.com", "daniel.scott@example.com",
                       "grace.lewis@example.com", "isaac.walker@example.com", "ava.king@example.com",
                       "mason.hill@example.com", "chloe.green@example.com", "benjamin.baker@example.com",
                       "zoe.adams@example.com", "william.nelson@example.com", "sarah.lee@example.com",
                       "jason.clark@example.com"),
    customer_phone = c("123-456-7890", "987-654-3210", "555-123-4567", "456-789-0123", "321-654-9870",
                       "234-567-8901", "876-543-2109", "345-678-9012", "789-012-3456", "890-123-4567",
                       "901-234-5678", "432-109-8765", "543-210-9876", "210-987-6543", "789-654-3210",
                       "987-654-3210", "654-321-0987", "321-098-7654", "098-765-4321", "876-543-2109",
                       "543-210-9876", "210-987-6543", "789-654-3210", "234-567-8901", "876-543-2109"),
    age=rnorm(25, mean = 50, sd = 15),
    sex=ifelse(rbinom(25, 1, .55) == 1, 'male', 'female')
)

# Orders table
Orders <- data.frame(
    order_id = 101:126,
    customer_id = c(1, 2, 1, 3, 4, 5, 6, 7, 8, 9,
                    10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                    20, 21, 22, 23, 24, 19),
    order_date = as.Date(c("2024-06-15", "2024-06-18", "2024-06-20", "2024-06-22", "2024-06-25",
                           "2024-06-28", "2024-06-30", "2024-07-02", "2024-07-05", "2024-07-08",
                           "2024-07-10", "2024-07-12", "2024-07-15", "2024-07-18", "2024-07-20",
                           "2024-07-22", "2024-07-25", "2024-07-28", "2024-07-30", "2024-08-02",
                           "2024-08-05", "2024-08-08", "2024-08-10", "2024-08-12", "2024-08-15", "2024-08-18")),
    total_amount = c(150.00, 200.00, 75.00, 300.00, 50.00,
                     400.00, 100.00, 250.00, 180.00, 300.00,
                     120.00, 80.00, 220.00, 150.00, 90.00,
                     200.00, 300.00, 400.00, 150.00, 125.00,
                     175.00, 250.00, 300.00, 400.00, 200.00, 125.00),
    product_id = c(201, 202, 203, 201, 202,
                   203, 201, 202, 203, 201,
                   202, 203, 201, 202, 203,
                   201, 202, 203, 201, 202,
                   203, 201, 202, 203, 204, 205)
)

# Products table
Products <- data.frame(
    product_id = c(201:210),
    product_name = c("Laptop", "Smartphone", "Headphones", "Tablet", "Smartwatch",
                     "Portable Speaker", "Keyboard", "Mouse", "Monitor", "Printer"),
    unit_price = c(1000.00, 800.00, 50.00, 600.00, 300.00,
                   80.00, 40.00, 20.00, 300.00, 150.00)
)


##save DFs as parquet files
lapply(c('Customers', 'Orders', 'Products'), 
        function(x) write_parquet(get(x), paste0(x, '.parquet')))

########################## duckdb ###################
# initialize duckdb in-memory database ##
con <- dbConnect(duckdb(), dbdir = ":memory:")

#this is a very basic sql qury
## note, 'read_parquet' is from duckdb, it is not 'true' sql
### '*' selects all columns
### 'WHERE' is akin to filter in dplyr speak
query <- "SELECT *
          FROM read_parquet('{paste(folder, 'Customers.parquet', sep='/')}')
          WHERE age > 40"

dbGetQuery(con, glue(query))

# Change the INNER to a LEFT join to see how they differ
query2 <- "SELECT c.*, o.*
          FROM read_parquet('{paste(folder, 'Customers.parquet', sep='/')}') AS c
          INNER JOIN read_parquet('{paste(folder, 'Orders.parquet', sep='/')}') AS o
            ON c.customer_id = o.Customer_id
          WHERE c.age > 40"

dbGetQuery(con, glue(query2))

# add the products table
query3 <- "SELECT c.*, o.*, p.*
          FROM read_parquet('{paste(folder, 'Customers.parquet', sep='/')}') AS c
          INNER JOIN read_parquet('{paste(folder, 'Orders.parquet', sep='/')}') AS o
            ON c.customer_id = o.Customer_id
          INNER JOIN read_parquet('{paste(folder, 'Products.parquet', sep='/')}') AS p
            ON p.product_id = o.product_id
          WHERE c.age > 40"

dbGetQuery(con, glue(query3))

######Subuery example####
query_sub1 <- "SELECT *
                FROM (SELECT *
                      FROM read_parquet('{paste(folder, 'Customers.parquet', sep='/')}')
                      WHERE age > 40) AS t
    "
dbGetQuery(con, glue(query_sub1))

##look, these are equivalent    queries:
all.equal(dbGetQuery(con, glue(query_sub1)), dbGetQuery(con, glue(query)))

####Common table expression example:####
cte <- "WITH cte AS (
    SELECT *
    FROM read_parquet('{paste(folder, 'Customers.parquet', sep='/')}')
    WHERE age > 40)"

query_cte <- glue("
    {cte}
    SELECT cte.*, o.*
    FROM read_parquet('{paste(folder, 'Orders.parquet', sep='/')}') AS o
    INNER JOIN cte AS cte
        ON cte.Customer_id = o.Customer_id
    ")
dbGetQuery(con, glue(query_cte))

#These are equivalent:
all.equal(dbGetQuery(con, glue(query_cte)), dbGetQuery(con, glue(query2))
)



