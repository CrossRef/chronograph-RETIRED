CREATE TABLE doi (
    doi VARCHAR(700) PRIMARY KEY,
    -- issued date lossily coerced into a datetime
    issuedDate DATETIME NULL,
    -- issued date represented as CrossRef date
    issuedString VARCHAR(10) NULL,
    redepositedDate DATETIME NULL,
    firstDepositedDate DATETIME NULL,
    resolved DATETIME NULL,
    firstResolution VARCHAR(1024) NULL,
    ultimateResolution VARCHAR(1024) NULL,
    firstResolutionLog DATETIME
);

CREATE TABLE state (
    name VARCHAR(1024) NOT NULL,
    theDate DATETIME NOT NULL
);