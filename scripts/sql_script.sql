-- Step 1.1 — Audit NULLs in the customers table
SELECT
  COUNT(*) AS total_rows,
  SUM(FirstName IS NULL) AS null_first_name,
  SUM(LastName IS NULL) AS null_last_name,
  SUM(DateOfBirth IS NULL) AS null_dob,
  SUM(AddressID IS NULL) AS null_address_id,
  SUM(CustomerTypeID IS NULL) AS null_customer_type
FROM customers;

-- Step 1.2 — Audit NULLs in the loans table
SELECT
  COUNT(*) AS total_rows,
  SUM(LoanStatusID IS NULL) AS null_status,
  SUM(PrincipalAmount IS NULL) AS null_principal,
  SUM(InterestRate IS NULL) AS null_rate,
  SUM(StartDate IS NULL) AS null_start_date,
  SUM(EstimatedEndDate IS NULL) AS null_end_date
FROM loans;

-- Step 1.3 — Check for duplicate transactions
SELECT TransactionID, COUNT(*) AS cnt
FROM transactions
GROUP BY TransactionID
HAVING cnt > 1
LIMIT 10;

-- Step 1.4 — Check for empty strings (disguised NULLs) in customers
SELECT
  SUM(FirstName = '') AS empty_first_name,
  SUM(LastName = '') AS empty_last_name,
  SUM(FirstName = '' OR LastName = '') AS total_bad_name_rows
FROM customers;

-- Step 1.5 — How many duplicate transactions exactly?
SELECT COUNT(*) AS total_duplicate_rows
FROM (
  SELECT TransactionID
  FROM transactions
  GROUP BY TransactionID
  HAVING COUNT(*) > 1
) AS dupes;

-- Step 1.6 — Create cleaned transactions table (remove duplicates)
CREATE TABLE transactions_clean AS
SELECT
  TransactionID,
  AccountOriginID,
  AccountDestinationID,
  TransactionTypeID,
  Amount,
  TransactionDate,
  BranchID,
  Description
FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY TransactionID
      ORDER BY TransactionDate
    ) AS row_num
  FROM transactions
) AS ranked
WHERE row_num = 1;

-- Step 1.7 — Verify your cleaning worked
-- Should return 0
SELECT COUNT(*) AS remaining_duplicates
FROM transactions_clean
GROUP BY TransactionID
HAVING COUNT(*) > 1;

-- Step 1.8 — CASE logic (categorize loan risk)
SELECT
  LoanID,
  PrincipalAmount,
  InterestRate,
  CASE
    WHEN InterestRate < 0.05  THEN 'Low Risk'
    WHEN InterestRate < 0.10  THEN 'Medium Risk'
    ELSE                           'High Risk'
  END AS RiskCategory
FROM loans
LIMIT 20;

-- Step 1.9 — Date fixes (standardize inconsistent dates)
SELECT
  LoanID,
  StartDate,
  EstimatedEndDate,
  CASE
    WHEN StartDate > CURDATE() THEN 'Future Start — Anomaly'
    WHEN EstimatedEndDate < StartDate THEN 'End Before Start — Anomaly'
    ELSE 'Valid'
  END AS DateCheck
FROM loans
WHERE StartDate > CURDATE()
   OR EstimatedEndDate < StartDate
LIMIT 10;

-- Step 1.10 — Update customers_clean with COALESCE baked in
CREATE TABLE customers_final AS
SELECT
  CustomerID,
  COALESCE(NULLIF(TRIM(FirstName), ''), 'Unknown') AS FirstName,
  COALESCE(NULLIF(TRIM(LastName), ''),  'Unknown') AS LastName,
  DateOfBirth,
  AddressID,
  CustomerTypeID
FROM customers;

-- Verify customers_final has no empty strings
SELECT COUNT(*) AS bad_name_rows
FROM customers_final
WHERE FirstName = 'Unknown' OR LastName = 'Unknown';


-- Step 2.1 — Verify referential integrity: accounts → customers
SELECT a.AccountID, a.CustomerID
FROM accounts a
LEFT JOIN customers c ON a.CustomerID = c.CustomerID
WHERE c.CustomerID IS NULL;

-- Step 2.2 — Verify referential integrity: loans → accounts
SELECT l.LoanID, l.AccountID
FROM loans l
LEFT JOIN accounts a ON l.AccountID = a.AccountID
WHERE a.AccountID IS NULL;

-- Step 2.3 — Verify referential integrity: transactions → accounts
SELECT t.TransactionID, t.AccountOriginID
FROM transactions_clean t
LEFT JOIN accounts a ON t.AccountOriginID = a.AccountID
WHERE a.AccountID IS NULL;

-- Step 2.4 — Orphan record detection: transactions → branches
SELECT t.TransactionID, t.BranchID
FROM transactions_clean t
LEFT JOIN branches b ON t.BranchID = b.BranchID
WHERE b.BranchID IS NULL;

-- Step 2.5 — Dimension table validation: LoanStatusID
SELECT l.LoanID, l.LoanStatusID
FROM loans l
LEFT JOIN loan_statuses ls ON l.LoanStatusID = ls.LoanStatusID
WHERE ls.LoanStatusID IS NULL;

-- Step 2.6 — Dimension table validation: AccountTypeID and AccountStatusID
SELECT
  a.AccountID,
  a.AccountTypeID,
  a.AccountStatusID,
  CASE WHEN at.AccountTypeID   IS NULL THEN 'Invalid Type'   ELSE 'Valid' END AS TypeCheck,
  CASE WHEN ast.AccountStatusID IS NULL THEN 'Invalid Status' ELSE 'Valid' END AS StatusCheck
FROM accounts a
LEFT JOIN account_types   at  ON a.AccountTypeID   = at.AccountTypeID
LEFT JOIN account_statuses ast ON a.AccountStatusID = ast.AccountStatusID
WHERE at.AccountTypeID IS NULL OR ast.AccountStatusID IS NULL;

-- Step 2.7 — Dimension table validation: CustomerTypeID
SELECT c.CustomerID, c.CustomerTypeID
FROM customers_final c
LEFT JOIN customer_types ct ON c.CustomerTypeID = ct.CustomerTypeID
WHERE ct.CustomerTypeID IS NULL;

-- Step 2.8 — Full referential integrity summary
SELECT 'accounts → customers'       AS relationship, COUNT(*) AS orphan_count FROM accounts a LEFT JOIN customers c ON a.CustomerID = c.CustomerID WHERE c.CustomerID IS NULL
UNION ALL
SELECT 'loans → accounts'           AS relationship, COUNT(*) AS orphan_count FROM loans l LEFT JOIN accounts a ON l.AccountID = a.AccountID WHERE a.AccountID IS NULL
UNION ALL
SELECT 'transactions → accounts'    AS relationship, COUNT(*) AS orphan_count FROM transactions_clean t LEFT JOIN accounts a ON t.AccountOriginID = a.AccountID WHERE a.AccountID IS NULL
UNION ALL
SELECT 'transactions → branches'    AS relationship, COUNT(*) AS orphan_count FROM transactions_clean t LEFT JOIN branches b ON t.BranchID = b.BranchID WHERE b.BranchID IS NULL
UNION ALL
SELECT 'loans → loan_statuses'      AS relationship, COUNT(*) AS orphan_count FROM loans l LEFT JOIN loan_statuses ls ON l.LoanStatusID = ls.LoanStatusID WHERE ls.LoanStatusID IS NULL
UNION ALL
SELECT 'customers → customer_types' AS relationship, COUNT(*) AS orphan_count FROM customers_final c LEFT JOIN customer_types ct ON c.CustomerTypeID = ct.CustomerTypeID WHERE ct.CustomerTypeID IS NULL;


-- Step 3.1 — Basic JOIN: customers → accounts
-- Link every customer to their account(s)
SELECT
  c.CustomerID,
  CONCAT(c.FirstName, ' ', c.LastName) AS CustomerName,
  ct.TypeName                           AS CustomerType,
  a.AccountID,
  at.TypeName                           AS AccountType,
  ast.StatusName                        AS AccountStatus,
  a.Balance
FROM customers_final   c
JOIN customer_types    ct  ON c.CustomerTypeID   = ct.CustomerTypeID
JOIN accounts          a   ON c.CustomerID       = a.CustomerID
JOIN account_types     at  ON a.AccountTypeID    = at.AccountTypeID
JOIN account_statuses  ast ON a.AccountStatusID  = ast.AccountStatusID
LIMIT 20;

-- Step 3.2 — Extend JOIN: add loans & loan status
-- Add loan data to the customer-account chain
SELECT
  c.CustomerID,
  CONCAT(c.FirstName, ' ', c.LastName) AS CustomerName,
  ct.TypeName                           AS CustomerType,
  a.AccountID,
  a.Balance                             AS AccountBalance,
  l.LoanID,
  l.PrincipalAmount,
  l.InterestRate,
  ls.StatusName                         AS LoanStatus
FROM customers_final   c
JOIN customer_types    ct  ON c.CustomerTypeID  = ct.CustomerTypeID
JOIN accounts          a   ON c.CustomerID      = a.CustomerID
JOIN loans             l   ON a.AccountID       = l.AccountID
JOIN loan_statuses     ls  ON l.LoanStatusID    = ls.LoanStatusID
LIMIT 20;

-- Step 3.3 — Master portfolio query (all tables unified)
-- Complete loan book: every dimension joined in one query
SELECT
  c.CustomerID,
  CONCAT(c.FirstName, ' ', c.LastName) AS CustomerName,
  ct.TypeName                           AS CustomerType,
  a.AccountID,
  at.TypeName                           AS AccountType,
  ast.StatusName                        AS AccountStatus,
  a.Balance                             AS AccountBalance,
  l.LoanID,
  l.PrincipalAmount,
  ROUND(l.InterestRate * 100, 2)        AS InterestRate_Pct,
  ls.StatusName                         AS LoanStatus,
  l.StartDate,
  l.EstimatedEndDate
FROM customers_final   c
JOIN customer_types    ct  ON c.CustomerTypeID  = ct.CustomerTypeID
JOIN accounts          a   ON c.CustomerID      = a.CustomerID
JOIN account_types     at  ON a.AccountTypeID   = at.AccountTypeID
JOIN account_statuses  ast ON a.AccountStatusID = ast.AccountStatusID
JOIN loans             l   ON a.AccountID       = l.AccountID
JOIN loan_statuses     ls  ON l.LoanStatusID    = ls.LoanStatusID
LIMIT 50;

-- Step 3.4 — Add transaction summary per account
-- Enrich master query with transaction count and total volume per account
SELECT
  c.CustomerID,
  CONCAT(c.FirstName, ' ', c.LastName) AS CustomerName,
  ct.TypeName                           AS CustomerType,
  a.AccountID,
  a.Balance                             AS AccountBalance,
  l.LoanID,
  l.PrincipalAmount,
  ls.StatusName                         AS LoanStatus,
  COUNT(t.TransactionID)                AS TotalTransactions,
  ROUND(SUM(t.Amount), 2)               AS TotalTransactionVolume
FROM customers_final   c
JOIN customer_types    ct  ON c.CustomerTypeID  = ct.CustomerTypeID
JOIN accounts          a   ON c.CustomerID      = a.CustomerID
JOIN loans             l   ON a.AccountID       = l.AccountID
JOIN loan_statuses     ls  ON l.LoanStatusID    = ls.LoanStatusID
LEFT JOIN transactions_clean t ON a.AccountID   = t.AccountOriginID
GROUP BY
  c.CustomerID, CustomerName, ct.TypeName,
  a.AccountID, a.Balance,
  l.LoanID, l.PrincipalAmount, ls.StatusName
LIMIT 20;

-- Step 3.5 — Customers with NO loans (LEFT JOIN)
-- Identify customers who have accounts but zero loans
-- Important for credit opportunity analysis
SELECT
  c.CustomerID,
  CONCAT(c.FirstName, ' ', c.LastName) AS CustomerName,
  ct.TypeName                           AS CustomerType,
  a.AccountID,
  a.Balance
FROM customers_final  c
JOIN customer_types   ct ON c.CustomerTypeID = ct.CustomerTypeID
JOIN accounts         a  ON c.CustomerID     = a.CustomerID
LEFT JOIN loans       l  ON a.AccountID      = l.AccountID
WHERE l.LoanID IS NULL
LIMIT 20;


-- Step 4.1 — Total capital exposure by loan status
-- How much principal is tied up in each loan status
SELECT
  ls.StatusName                        AS LoanStatus,
  COUNT(l.LoanID)                      AS TotalLoans,
  ROUND(SUM(l.PrincipalAmount), 2)     AS TotalPrincipal,
  ROUND(AVG(l.PrincipalAmount), 2)     AS AvgLoanSize
FROM loans         l
JOIN loan_statuses ls ON l.LoanStatusID = ls.LoanStatusID
GROUP BY ls.StatusName
ORDER BY TotalPrincipal DESC;

-- Step 4.2 — Expected interest yield by loan status
-- Expected yield = PrincipalAmount × InterestRate per loan status
SELECT
  ls.StatusName                                        AS LoanStatus,
  ROUND(SUM(l.PrincipalAmount), 2)                     AS TotalPrincipal,
  ROUND(SUM(l.PrincipalAmount * l.InterestRate), 2)    AS ExpectedYield,
  ROUND(AVG(l.InterestRate * 100), 2)                  AS AvgInterestRate_Pct
FROM loans         l
JOIN loan_statuses ls ON l.LoanStatusID = ls.LoanStatusID
GROUP BY ls.StatusName
ORDER BY ExpectedYield DESC;

-- Step 4.3 — Portfolio health scorecard
-- Full portfolio summary: exposure, yield, loan count and % share
SELECT
  ls.StatusName                                              AS LoanStatus,
  COUNT(l.LoanID)                                            AS LoanCount,
  ROUND(SUM(l.PrincipalAmount), 2)                           AS TotalPrincipal,
  ROUND(SUM(l.PrincipalAmount * l.InterestRate), 2)          AS ExpectedYield,
  ROUND(COUNT(l.LoanID) * 100.0 / SUM(COUNT(l.LoanID)) 
        OVER (), 2)                                          AS PctOfPortfolio
FROM loans         l
JOIN loan_statuses ls ON l.LoanStatusID = ls.LoanStatusID
GROUP BY ls.StatusName
ORDER BY TotalPrincipal DESC;

-- Step 4.4 — Exposure breakdown by customer segment
-- Which customer type carries the most loan exposure?
SELECT
  ct.TypeName                                           AS CustomerType,
  COUNT(l.LoanID)                                       AS TotalLoans,
  ROUND(SUM(l.PrincipalAmount), 2)                      AS TotalExposure,
  ROUND(SUM(l.PrincipalAmount * l.InterestRate), 2)     AS ExpectedYield,
  ROUND(AVG(l.InterestRate * 100), 2)                   AS AvgInterestRate_Pct
FROM customers_final  c
JOIN customer_types   ct ON c.CustomerTypeID  = ct.CustomerTypeID
JOIN accounts         a  ON c.CustomerID      = a.CustomerID
JOIN loans            l  ON a.AccountID       = l.AccountID
GROUP BY ct.TypeName
ORDER BY TotalExposure DESC;

-- Step 4.5 — Overdue loan exposure (capital at risk)
-- Isolate only overdue loans — the true capital at risk figure
SELECT
  ROUND(SUM(l.PrincipalAmount), 2)                    AS OverduePrincipal,
  ROUND(SUM(l.PrincipalAmount * l.InterestRate), 2)   AS OverdueExpectedYield,
  COUNT(l.LoanID)                                     AS OverdueLoanCount,
  ROUND(AVG(l.InterestRate * 100), 2)                 AS AvgOverdueRate_Pct
FROM loans         l
JOIN loan_statuses ls ON l.LoanStatusID = ls.LoanStatusID
WHERE ls.StatusName = 'Overdue';


-- Step 5.1 — Aggregate total loan debt per customer
-- Total outstanding principal owed by each customer
SELECT
  c.CustomerID,
  CONCAT(c.FirstName, ' ', c.LastName)  AS CustomerName,
  ct.TypeName                            AS CustomerType,
  ROUND(SUM(l.PrincipalAmount), 2)       AS TotalLoanDebt
FROM customers_final  c
JOIN customer_types   ct ON c.CustomerTypeID = ct.CustomerTypeID
JOIN accounts         a  ON c.CustomerID     = a.CustomerID
JOIN loans            l  ON a.AccountID      = l.AccountID
GROUP BY c.CustomerID, CustomerName, ct.TypeName
ORDER BY TotalLoanDebt DESC
LIMIT 20;

-- Step 5.2 — Aggregate total liquid assets per customer
-- Total account balance available per customer
SELECT
  c.CustomerID,
  CONCAT(c.FirstName, ' ', c.LastName)  AS CustomerName,
  ROUND(SUM(a.Balance), 2)               AS TotalLiquidAssets
FROM customers_final  c
JOIN accounts         a  ON c.CustomerID = a.CustomerID
GROUP BY c.CustomerID, CustomerName
ORDER BY TotalLiquidAssets DESC
LIMIT 20;

-- Step 5.3 — Liquidity gap calculation
-- LiquidityGap = TotalLoanDebt - TotalLiquidAssets
-- Positive value = insolvent (owes more than they have)
SELECT
  c.CustomerID,
  CONCAT(c.FirstName, ' ', c.LastName)      AS CustomerName,
  ct.TypeName                                AS CustomerType,
  ROUND(SUM(l.PrincipalAmount), 2)           AS TotalLoanDebt,
  ROUND(SUM(DISTINCT a.Balance), 2)          AS TotalLiquidAssets,
  ROUND(SUM(l.PrincipalAmount) - 
        SUM(DISTINCT a.Balance), 2)          AS LiquidityGap
FROM customers_final  c
JOIN customer_types   ct ON c.CustomerTypeID = ct.CustomerTypeID
JOIN accounts         a  ON c.CustomerID     = a.CustomerID
JOIN loans            l  ON a.AccountID      = l.AccountID
GROUP BY c.CustomerID, CustomerName, ct.TypeName
ORDER BY LiquidityGap DESC
LIMIT 20;

-- Step 5.4 — DAR (Debt-to-Asset Ratio) calculation
-- DAR = TotalLoanDebt / TotalLiquidAssets
-- DAR > 1 means debt exceeds assets — insolvent
SELECT
  c.CustomerID,
  CONCAT(c.FirstName, ' ', c.LastName)         AS CustomerName,
  ct.TypeName                                   AS CustomerType,
  ROUND(SUM(l.PrincipalAmount), 2)              AS TotalLoanDebt,
  ROUND(SUM(DISTINCT a.Balance), 2)             AS TotalLiquidAssets,
  ROUND(SUM(l.PrincipalAmount) /
        NULLIF(SUM(DISTINCT a.Balance), 0), 4)  AS DAR
FROM customers_final  c
JOIN customer_types   ct ON c.CustomerTypeID = ct.CustomerTypeID
JOIN accounts         a  ON c.CustomerID     = a.CustomerID
JOIN loans            l  ON a.AccountID      = l.AccountID
GROUP BY c.CustomerID, CustomerName, ct.TypeName
ORDER BY DAR DESC
LIMIT 20;

-- Step 5.5 — Red Flag List (insolvent borrowers only)
-- Official Red Flag List: DAR > 1 AND overdue loan status
SELECT
  c.CustomerID,
  CONCAT(c.FirstName, ' ', c.LastName)         AS CustomerName,
  ct.TypeName                                   AS CustomerType,
  ROUND(SUM(l.PrincipalAmount), 2)              AS TotalLoanDebt,
  ROUND(SUM(DISTINCT a.Balance), 2)             AS TotalLiquidAssets,
  ROUND(SUM(l.PrincipalAmount) /
        NULLIF(SUM(DISTINCT a.Balance), 0), 4)  AS DAR,
  ROUND(SUM(l.PrincipalAmount) -
        SUM(DISTINCT a.Balance), 2)             AS UncollateralizedDebt
FROM customers_final  c
JOIN customer_types   ct ON c.CustomerTypeID  = ct.CustomerTypeID
JOIN accounts         a  ON c.CustomerID      = a.CustomerID
JOIN loans            l  ON a.AccountID       = l.AccountID
JOIN loan_statuses    ls ON l.LoanStatusID    = ls.LoanStatusID
WHERE ls.StatusName = 'Overdue'
GROUP BY c.CustomerID, CustomerName, ct.TypeName
HAVING DAR > 1
ORDER BY UncollateralizedDebt DESC;


-- Step 6.1 — First CTE: default rate per customer segment
-- CTE calculates default rate % for each customer type
WITH SegmentDefaults AS (
  SELECT
    ct.TypeName                                         AS CustomerType,
    COUNT(l.LoanID)                                     AS TotalLoans,
    SUM(CASE WHEN ls.StatusName = 'Overdue' 
             THEN 1 ELSE 0 END)                         AS OverdueLoans,
    ROUND(SUM(CASE WHEN ls.StatusName = 'Overdue' 
                   THEN 1 ELSE 0 END) * 100.0 /
          NULLIF(COUNT(l.LoanID), 0), 2)                AS DefaultRate_Pct
  FROM customers_final  c
  JOIN customer_types   ct ON c.CustomerTypeID = ct.CustomerTypeID
  JOIN accounts         a  ON c.CustomerID     = a.CustomerID
  JOIN loans            l  ON a.AccountID      = l.AccountID
  JOIN loan_statuses    ls ON l.LoanStatusID   = ls.LoanStatusID
  GROUP BY ct.TypeName
)
SELECT * FROM SegmentDefaults
ORDER BY DefaultRate_Pct DESC;

-- Step 6.2 — RANK() window function on segment default rates
-- Rank customer segments by default rate using RANK()
WITH SegmentDefaults AS (
  SELECT
    ct.TypeName                                         AS CustomerType,
    COUNT(l.LoanID)                                     AS TotalLoans,
    SUM(CASE WHEN ls.StatusName = 'Overdue' 
             THEN 1 ELSE 0 END)                         AS OverdueLoans,
    ROUND(SUM(CASE WHEN ls.StatusName = 'Overdue' 
                   THEN 1 ELSE 0 END) * 100.0 /
          NULLIF(COUNT(l.LoanID), 0), 2)                AS DefaultRate_Pct
  FROM customers_final  c
  JOIN customer_types   ct ON c.CustomerTypeID = ct.CustomerTypeID
  JOIN accounts         a  ON c.CustomerID     = a.CustomerID
  JOIN loans            l  ON a.AccountID      = l.AccountID
  JOIN loan_statuses    ls ON l.LoanStatusID   = ls.LoanStatusID
  GROUP BY ct.TypeName
)
SELECT
  CustomerType,
  TotalLoans,
  OverdueLoans,
  DefaultRate_Pct,
  RANK()        OVER (ORDER BY DefaultRate_Pct DESC) AS RankByDefault,
  DENSE_RANK()  OVER (ORDER BY DefaultRate_Pct DESC) AS DenseRankByDefault
FROM SegmentDefaults;

-- Step 6.3 — Branch-level default profiling with RANK()
-- Rank each branch by its overdue loan count
WITH BranchRisk AS (
  SELECT
    b.BranchID,
    b.BranchName,
    COUNT(l.LoanID)                                     AS TotalLoans,
    SUM(CASE WHEN ls.StatusName = 'Overdue' 
             THEN 1 ELSE 0 END)                         AS OverdueLoans,
    ROUND(SUM(CASE WHEN ls.StatusName = 'Overdue' 
                   THEN 1 ELSE 0 END) * 100.0 /
          NULLIF(COUNT(l.LoanID), 0), 2)                AS DefaultRate_Pct
  FROM branches         b
  JOIN transactions_clean t  ON b.BranchID      = t.BranchID
  JOIN accounts          a  ON t.AccountOriginID = a.AccountID
  JOIN loans             l  ON a.AccountID       = l.AccountID
  JOIN loan_statuses     ls ON l.LoanStatusID    = ls.LoanStatusID
  GROUP BY b.BranchID, b.BranchName
)
SELECT
  BranchID,
  BranchName,
  TotalLoans,
  OverdueLoans,
  DefaultRate_Pct,
  RANK() OVER (ORDER BY DefaultRate_Pct DESC) AS RiskRank
FROM BranchRisk
ORDER BY RiskRank
LIMIT 20;

-- Step 6.4 — LAG & LEAD: month-over-month transaction trend
-- Compare each month's transaction volume to previous and next month
WITH MonthlyVolume AS (
  SELECT
    DATE_FORMAT(TransactionDate, '%Y-%m')   AS TxMonth,
    COUNT(TransactionID)                    AS TxCount,
    ROUND(SUM(Amount), 2)                   AS TotalVolume
  FROM transactions_clean
  GROUP BY TxMonth
)
SELECT
  TxMonth,
  TxCount,
  TotalVolume,
  LAG(TotalVolume)  OVER (ORDER BY TxMonth) AS PrevMonthVolume,
  LEAD(TotalVolume) OVER (ORDER BY TxMonth) AS NextMonthVolume,
  ROUND(TotalVolume - 
        LAG(TotalVolume) OVER (ORDER BY TxMonth), 2) AS MoM_Change
FROM MonthlyVolume
ORDER BY TxMonth;

-- Step 6.5 — Running total of loan exposure over time
-- Cumulative principal exposure as loans were issued over time
SELECT
  DATE_FORMAT(l.StartDate, '%Y-%m')          AS LoanMonth,
  ROUND(SUM(l.PrincipalAmount), 2)           AS MonthlyExposure,
  ROUND(SUM(SUM(l.PrincipalAmount)) 
        OVER (ORDER BY DATE_FORMAT(l.StartDate, '%Y-%m')), 2) AS RunningTotalExposure
FROM loans l
GROUP BY LoanMonth
ORDER BY LoanMonth;


-- Step 7.1 — Baseline: EXPLAIN before any index
EXPLAIN
SELECT
  l.LoanID,
  l.PrincipalAmount,
  l.LoanStatusID
FROM loans l
WHERE l.LoanStatusID = 2;
  
-- Step 7.2 — Create B-Tree composite index on loans
CREATE INDEX idx_loans_status_account
ON loans (LoanStatusID, AccountID);

-- Step 7.3 — EXPLAIN after index on loans
EXPLAIN
SELECT
  l.LoanID,
  l.PrincipalAmount,
  l.LoanStatusID
FROM loans l
WHERE l.LoanStatusID = 2;

-- Step 7.4 — Create composite index on transactions_clean
CREATE INDEX idx_transactions_account
ON transactions_clean (AccountOriginID);

-- Step 7.5 — EXPLAIN ANALYZE on transactions query
EXPLAIN ANALYZE
SELECT
  t.AccountOriginID,
  COUNT(t.TransactionID)     AS TxCount,
  ROUND(SUM(t.Amount), 2)    AS TotalVolume
FROM transactions_clean t
WHERE t.TransactionDate >= '2023-01-01'
GROUP BY t.AccountOriginID
ORDER BY TotalVolume DESC
LIMIT 20;

-- Step 7.6 — Create index on customers_final for JOIN speed
CREATE INDEX idx_customers_type
ON customers_final (CustomerTypeID);

-- Step 7.7 — Create index on accounts for JOIN speed
CREATE INDEX idx_accounts_customer
ON accounts (CustomerID);

-- Step 7.8 — EXPLAIN on master portfolio query with all indexes active
EXPLAIN
SELECT
  c.CustomerID,
  CONCAT(c.FirstName, ' ', c.LastName)   AS CustomerName,
  ct.TypeName                             AS CustomerType,
  a.AccountID,
  a.Balance                              AS AccountBalance,
  l.LoanID,
  l.PrincipalAmount,
  ls.StatusName                          AS LoanStatus
FROM customers_final   c
JOIN customer_types    ct  ON c.CustomerTypeID  = ct.CustomerTypeID
JOIN accounts          a   ON c.CustomerID      = a.CustomerID
JOIN loans             l   ON a.AccountID       = l.AccountID
JOIN loan_statuses     ls  ON l.LoanStatusID    = ls.LoanStatusID
LIMIT 50;

-- Step 7.9 — Verify all indexes registered in schema
SELECT
  TABLE_NAME,
  INDEX_NAME,
  COLUMN_NAME,
  INDEX_TYPE
FROM INFORMATION_SCHEMA.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME IN ('loans', 'transactions_clean', 'customers_final')
ORDER BY TABLE_NAME, INDEX_NAME;


-- Step 8a.1 — Executive portfolio health view
-- Master view centralizing capital exposure, yield and loan status
CREATE VIEW vw_PortfolioHealth AS
SELECT
  ls.StatusName                                        AS LoanStatus,
  COUNT(l.LoanID)                                      AS TotalLoans,
  ROUND(SUM(l.PrincipalAmount), 2)                     AS TotalPrincipal,
  ROUND(SUM(l.PrincipalAmount * l.InterestRate), 2)    AS ExpectedYield,
  ROUND(COUNT(l.LoanID) * 100.0 /
        SUM(COUNT(l.LoanID)) OVER (), 2)               AS PctOfPortfolio
FROM loans         l
JOIN loan_statuses ls ON l.LoanStatusID = ls.LoanStatusID
GROUP BY ls.StatusName;

-- Get full portfolio health
SELECT * FROM vw_PortfolioHealth;
-- Filter just overdue
SELECT * FROM vw_PortfolioHealth WHERE LoanStatus = 'Overdue';
-- Order by principal
SELECT * FROM vw_PortfolioHealth ORDER BY TotalPrincipal DESC;

-- Step 8a.2 — Red Flag List view
-- Persistent view of all insolvent borrowers (DAR > 1, Overdue)
CREATE VIEW vw_RedFlagCustomers AS
SELECT
  c.CustomerID,
  CONCAT(c.FirstName, ' ', c.LastName)          AS CustomerName,
  ct.TypeName                                    AS CustomerType,
  ROUND(SUM(l.PrincipalAmount), 2)               AS TotalLoanDebt,
  ROUND(SUM(DISTINCT a.Balance), 2)              AS TotalLiquidAssets,
  ROUND(SUM(l.PrincipalAmount) /
        NULLIF(SUM(DISTINCT a.Balance), 0), 4)   AS DAR,
  ROUND(SUM(l.PrincipalAmount) -
        SUM(DISTINCT a.Balance), 2)              AS UncollateralizedDebt
FROM customers_final  c
JOIN customer_types   ct ON c.CustomerTypeID  = ct.CustomerTypeID
JOIN accounts         a  ON c.CustomerID      = a.CustomerID
JOIN loans            l  ON a.AccountID       = l.AccountID
JOIN loan_statuses    ls ON l.LoanStatusID    = ls.LoanStatusID
WHERE ls.StatusName = 'Overdue'
GROUP BY c.CustomerID, CustomerName, ct.TypeName
HAVING DAR > 1
ORDER BY UncollateralizedDebt DESC;

-- Full Red Flag List
SELECT * FROM vw_RedFlagCustomers;
-- Only Large Enterprise red flags
SELECT * FROM vw_RedFlagCustomers WHERE CustomerType = 'Large Enterprise';
-- Top 5 most urgent
SELECT * FROM vw_RedFlagCustomers LIMIT 5;

-- Step 8a.3 — Segment rankings view
-- Persistent view of default rate rankings by customer segment
CREATE VIEW vw_SegmentRankings AS
SELECT
  ct.TypeName                                          AS CustomerType,
  COUNT(l.LoanID)                                      AS TotalLoans,
  SUM(CASE WHEN ls.StatusName = 'Overdue'
           THEN 1 ELSE 0 END)                          AS OverdueLoans,
  ROUND(SUM(CASE WHEN ls.StatusName = 'Overdue'
                 THEN 1 ELSE 0 END) * 100.0 /
        NULLIF(COUNT(l.LoanID), 0), 2)                 AS DefaultRate_Pct,
  RANK() OVER (ORDER BY
        SUM(CASE WHEN ls.StatusName = 'Overdue'
                 THEN 1 ELSE 0 END) * 100.0 /
        NULLIF(COUNT(l.LoanID), 0) DESC)               AS RiskRank
FROM customers_final  c
JOIN customer_types   ct ON c.CustomerTypeID = ct.CustomerTypeID
JOIN accounts         a  ON c.CustomerID     = a.CustomerID
JOIN loans            l  ON a.AccountID      = l.AccountID
JOIN loan_statuses    ls ON l.LoanStatusID   = ls.LoanStatusID
GROUP BY ct.TypeName;

SELECT * FROM vw_SegmentRankings;

-- Step 8a.4 — Stored Procedure: Credit Health Score
-- Stored procedure that calculates a dynamic credit health score
-- Score is based on transaction frequency and repayment history
DELIMITER $$

CREATE PROCEDURE sp_CreditHealthScore(IN p_CustomerID INT)
BEGIN
  SELECT
    c.CustomerID,
    CONCAT(c.FirstName, ' ', c.LastName)              AS CustomerName,
    ct.TypeName                                        AS CustomerType,
    COUNT(DISTINCT l.LoanID)                           AS TotalLoans,
    SUM(CASE WHEN ls.StatusName = 'Paid Off'
             THEN 1 ELSE 0 END)                        AS PaidOffLoans,
    SUM(CASE WHEN ls.StatusName = 'Overdue'
             THEN 1 ELSE 0 END)                        AS OverdueLoans,
    COUNT(DISTINCT t.TransactionID)                    AS TotalTransactions,
    ROUND(
      (SUM(CASE WHEN ls.StatusName = 'Paid Off'
                THEN 1 ELSE 0 END) * 50) +
      (COUNT(DISTINCT t.TransactionID) * 0.1) -
      (SUM(CASE WHEN ls.StatusName = 'Overdue'
                THEN 1 ELSE 0 END) * 30)
    , 2)                                               AS CreditHealthScore
  FROM customers_final    c
  JOIN customer_types     ct ON c.CustomerTypeID   = ct.CustomerTypeID
  JOIN accounts           a  ON c.CustomerID       = a.CustomerID
  JOIN loans              l  ON a.AccountID        = l.AccountID
  JOIN loan_statuses      ls ON l.LoanStatusID     = ls.LoanStatusID
  LEFT JOIN transactions_clean t ON a.AccountID    = t.AccountOriginID
  WHERE c.CustomerID = p_CustomerID
  GROUP BY c.CustomerID, CustomerName, ct.TypeName;
END$$

DELIMITER ;

-- CustomerID that definitely has loans:
SELECT DISTINCT c.CustomerID, c.FirstName, c.LastName
FROM customers_final  c
JOIN accounts         a  ON c.CustomerID = a.CustomerID
JOIN loans            l  ON a.AccountID  = l.AccountID
LIMIT 10;

-- Call the procedure for any customer by their ID
CALL sp_CreditHealthScore(11000);

-- Step 8a.5 — Verify all views registered in schema
-- Confirm all three views exist in the database
SELECT
  TABLE_NAME    AS ViewName,
  VIEW_DEFINITION
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = DATABASE();

-- Step 8b.1 — Baseline expected yield (current rates)
-- Current yield at existing interest rates — the benchmark
SELECT
  ls.StatusName                                       AS LoanStatus,
  COUNT(l.LoanID)                                     AS TotalLoans,
  ROUND(SUM(l.PrincipalAmount), 2)                    AS TotalPrincipal,
  ROUND(SUM(l.PrincipalAmount * l.InterestRate), 2)   AS CurrentYield
FROM loans         l
JOIN loan_statuses ls ON l.LoanStatusID = ls.LoanStatusID
GROUP BY ls.StatusName
ORDER BY TotalPrincipal DESC;

-- Step 8b.2 — What-If simulation: ±1% and ±2% rate shifts
-- Simulate yield impact across 4 rate scenarios in one query
SELECT
  ls.StatusName                                              AS LoanStatus,
  ROUND(SUM(l.PrincipalAmount * l.InterestRate), 2)         AS CurrentYield,
  ROUND(SUM(l.PrincipalAmount * (l.InterestRate - 0.01)), 2) AS Yield_Minus1Pct,
  ROUND(SUM(l.PrincipalAmount * (l.InterestRate - 0.02)), 2) AS Yield_Minus2Pct,
  ROUND(SUM(l.PrincipalAmount * (l.InterestRate + 0.01)), 2) AS Yield_Plus1Pct,
  ROUND(SUM(l.PrincipalAmount * (l.InterestRate + 0.02)), 2) AS Yield_Plus2Pct
FROM loans         l
JOIN loan_statuses ls ON l.LoanStatusID = ls.LoanStatusID
GROUP BY ls.StatusName
ORDER BY ls.StatusName;

 -- Step 8b.3 — Revenue impact delta (how much is gained or lost)
 -- Calculate exact revenue gain/loss vs current yield per scenario
SELECT
  ls.StatusName                                                         AS LoanStatus,
  ROUND(SUM(l.PrincipalAmount * l.InterestRate), 2)                    AS CurrentYield,
  ROUND(SUM(l.PrincipalAmount * (l.InterestRate + 0.01)) -
        SUM(l.PrincipalAmount * l.InterestRate), 2)                    AS Delta_Plus1Pct,
  ROUND(SUM(l.PrincipalAmount * (l.InterestRate + 0.02)) -
        SUM(l.PrincipalAmount * l.InterestRate), 2)                    AS Delta_Plus2Pct,
  ROUND(SUM(l.PrincipalAmount * (l.InterestRate - 0.01)) -
        SUM(l.PrincipalAmount * l.InterestRate), 2)                    AS Delta_Minus1Pct,
  ROUND(SUM(l.PrincipalAmount * (l.InterestRate - 0.02)) -
        SUM(l.PrincipalAmount * l.InterestRate), 2)                    AS Delta_Minus2Pct
FROM loans         l
JOIN loan_statuses ls ON l.LoanStatusID = ls.LoanStatusID
GROUP BY ls.StatusName
ORDER BY ls.StatusName;

-- Step 8b.4 — ECL (Expected Credit Loss) simulation on overdue loans
-- ECL = PrincipalAmount × InterestRate × probability of default
-- Simulate ECL under different rate environments for overdue loans only
SELECT
  ROUND(SUM(l.PrincipalAmount), 2)                              AS OverduePrincipal,
  ROUND(SUM(l.PrincipalAmount * l.InterestRate), 2)             AS ECL_CurrentRate,
  ROUND(SUM(l.PrincipalAmount * (l.InterestRate + 0.01)), 2)    AS ECL_Plus1Pct,
  ROUND(SUM(l.PrincipalAmount * (l.InterestRate + 0.02)), 2)    AS ECL_Plus2Pct,
  ROUND(SUM(l.PrincipalAmount * (l.InterestRate - 0.01)), 2)    AS ECL_Minus1Pct,
  ROUND(SUM(l.PrincipalAmount * (l.InterestRate - 0.02)), 2)    AS ECL_Minus2Pct
FROM loans         l
JOIN loan_statuses ls ON l.LoanStatusID = ls.LoanStatusID
WHERE ls.StatusName = 'Overdue';

-- Step 8b.5 — Stored Procedure: dynamic What-If simulation
-- Accepts any rate shift as input and returns full portfolio simulation
DELIMITER $$

CREATE PROCEDURE sp_WhatIfSimulation(IN p_RateShift DECIMAL(5,4))
BEGIN
  SELECT
    ls.StatusName                                               AS LoanStatus,
    COUNT(l.LoanID)                                             AS TotalLoans,
    ROUND(SUM(l.PrincipalAmount), 2)                            AS TotalPrincipal,
    ROUND(SUM(l.PrincipalAmount * l.InterestRate), 2)           AS CurrentYield,
    ROUND(SUM(l.PrincipalAmount * 
          (l.InterestRate + p_RateShift)), 2)                   AS SimulatedYield,
    ROUND(SUM(l.PrincipalAmount * (l.InterestRate + p_RateShift)) -
          SUM(l.PrincipalAmount * l.InterestRate), 2)           AS YieldDelta
  FROM loans         l
  JOIN loan_statuses ls ON l.LoanStatusID = ls.LoanStatusID
  GROUP BY ls.StatusName;
END$$

DELIMITER ;

-- Call with any rate shift value
CALL sp_WhatIfSimulation(0.01);   -- +1% rate increase
CALL sp_WhatIfSimulation(-0.01);  -- -1% rate decrease
CALL sp_WhatIfSimulation(0.02);   -- +2% rate increase
CALL sp_WhatIfSimulation(-0.02);  -- -2% rate decrease
