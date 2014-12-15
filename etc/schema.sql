CREATE TABLE olddoi (
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

-- The party who inseted the data
CREATE TABLE sources (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    ident VARCHAR(128) NOT NULL UNIQUE,
    name TEXT
);

-- Types of events
CREATE TABLE types (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    ident VARCHAR(128) NOT NULL UNIQUE,
    name TEXT,
    arg1desc TEXT,
    arg2desc TEXT,
    arg3desc TEXT
);

CREATE TABLE doi (
    doi VARCHAR(700) PRIMARY KEY,
    id INTEGER AUTO_INCREMENT,
    UNIQUE(DOI),
    UNIQUE(ID));

CREATE TABLE events (
    id  INTEGER AUTO_INCREMENT PRIMARY KEY,
    doi INT NOT NULL REFERENCES DOI(ID),

    -- number of events that this represents
    -- normally 1, but this can be an aggregate for something else
    count INTEGER NOT NULL DEFAULT 1,

    -- datetime of event
    -- null for 'all time'
    event DATETIME NULL,

    -- datetime this was inserted
    inserted DATETIME NOT NULL,

    source INT NOT NULL REFERENCES sources(id) ,
    type INT NOT NULL REFERENCES types(id),

    arg1 TEXT,
    arg2 TEXT,
    arg3 TEXT
);

create table referrer_domain_events (
   event DATETIME NULL, 
   count INTEGER NOT NULL,
   subdomain VARCHAR(128) NOT NULL UNIQUE,
   domain VARCHAR(128) NOT NULL
);

create table referrer_subdomain_events (
   event DATETIME NULL, 
   count INTEGER NOT NULL,
   subdomain VARCHAR(128) NOT NULL UNIQUE,
   domain VARCHAR(128) NOT NULL,
   source INT NOT NULL REFERENCES sources(id) ,
   type INT NOT NULL REFERENCES types(id)
);

insert into sources (ident, name) values ("CrossRefMetadata", "CrossRef Metadata");
insert into sources (ident, name) values ("CrossRefLogs", "CrossRef Resolution Logs");
insert into sources (ident, name) values ("CrossRefRobot", "CrossRef Robot");

insert into types (ident, name, arg1desc) values ("issued", "Publisher Issue date", "CrossRef extended date");
insert into types (ident, name) values ("deposited", "Publisher first deposited with CrossRef");
insert into types (ident, name) values ("updated", "Publisher most recently updated CrossRef metadata");
insert into types (ident, name, arg1desc, arg2desc) values ("first-resolution-test", "First attempt DOI resolution test", "Initial resolution URL", "Ultimate resolution URL");

insert into types (ident, name) values ("first-resolution", "First DOI resolution");
insert into types (ident, name) values ("total-resolutions", "Total resolutions count");

insert into types (ident, name) values ("daily-resolutions", "Daily resolutions count");
insert into types (ident, name) values ("monthly-resolutions", "Monthly resolutions count");
insert into types (ident, name) values ("yearly-resolutions", "Yearly resolutions count");

insert into types (ident, name) values ("daily-referral", "Daily referral count");
insert into types (ident, name) values ("monthly-referral", "Monthly referral count");
insert into types (ident, name) values ("yearly-referral", "Yearly referral count");


