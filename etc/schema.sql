-- Last run date etc.
CREATE TABLE state (
    name VARCHAR(1024) NOT NULL,
    theDate DATETIME NOT NULL
);

-- The party who inserted the data
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
    milestone BOOL NOT NULL DEFAULT FALSE,
    arg1desc TEXT,
    arg2desc TEXT,
    arg3desc TEXT
);

CREATE TABLE tokens (
    token VARCHAR(128) PRIMARY KEY,
    -- comma separated source names
    allowed_sources TEXT,
    -- comma separated type names
    allowed_types TEXT
);

-- Storage of entire timeline per DOI.
-- Template for sharded event timeline tables. Will be copied to create new shards.
CREATE TABLE timeline_shard_template (
    id  INTEGER AUTO_INCREMENT PRIMARY KEY,
    doi VARCHAR(700),

    -- datetime this was inserted
    inserted DATETIME NOT NULL, 

    source INT NOT NULL REFERENCES sources(id) ,
    type INT NOT NULL REFERENCES types(id),

    -- JSON of date -> count
    timeline MEDIUMBLOB
) ENGINE = myisam;

-- type isn't indexed because it's the basis of the shard table identity, so is always known.
CREATE UNIQUE INDEX event_timelines_doi_source_type_isam ON timeline_shard_template (doi);

-- This is a template for sharded events tables. Will be copied to create new shards.
CREATE TABLE event_shard_template (
    id  INTEGER AUTO_INCREMENT PRIMARY KEY,
    doi VARCHAR(700),

    -- number of events that this represents
    -- normally 1, but this can be an aggregate for something else
    count INTEGER NOT NULL DEFAULT 1,

    -- datetime of event
    -- null for 'all time'
    event DATETIME NULL,

    -- datetime this was inserted
    inserted DATETIME NOT NULL,

    source INT NOT NULL REFERENCES sources(id),

    -- Redundant but stored to make sharding a generic + easier + backward compatible.
    type INT NOT NULL REFERENCES types(id),

    arg1 TEXT,
    arg2 TEXT,
    arg3 TEXT
) ENGINE = myisam;

-- no uniques in the events table.
ALTER TABLE event_shard_template
add INDEX events_te (event);

-- This is a template for sharded milestone tables. Will be copied to create new shards.
CREATE TABLE milestone_shard_template (
    id  INTEGER AUTO_INCREMENT PRIMARY KEY,
    doi VARCHAR(700),

    -- number of events that this represents
    -- normally 1, but this can be an aggregate for something else
    count INTEGER NOT NULL DEFAULT 1,

    -- datetime of event
    -- null for 'all time'
    event DATETIME NULL,

    -- datetime this was inserted
    inserted DATETIME NOT NULL,

    source INT NOT NULL REFERENCES sources(id),

    -- Redundant but stored to make sharding a generic + easier + backward compatible.
    type INT NOT NULL REFERENCES types(id),

    arg1 TEXT,
    arg2 TEXT,
    arg3 TEXT
) ENGINE = myisam;

ALTER TABLE milestone_shard_template
add UNIQUE INDEX events_tstt (doi, source), -- type is implicit by virtue of sharding
add INDEX events_te (event);

-- This is a template for sharded fact tables. Will be copied to create new shards.
CREATE TABLE fact_shard_template (
    id  INTEGER AUTO_INCREMENT PRIMARY KEY,
    doi VARCHAR(700),

    -- number of events that this represents
    -- normally 1, but this can be an aggregate for something else
    count INTEGER NOT NULL DEFAULT 1,

    -- datetime of event
    -- null for 'all time'
    event DATETIME NULL,

    -- datetime this was inserted
    inserted DATETIME NOT NULL,

    source INT NOT NULL REFERENCES sources(id),

    -- Redundant but stored to make sharding a generic + easier + backward compatible.
    type INT NOT NULL REFERENCES types(id),

    arg1 TEXT,
    arg2 TEXT,
    arg3 TEXT
) ENGINE = myisam;

ALTER TABLE fact_shard_template
add UNIQUE INDEX events_tstt (doi, source), -- type is implicit by virtue of sharding
add INDEX events_te (event);

-- Storage of entire timeline per referrer domain.
CREATE TABLE referrer_domain_timelines (
    id  INTEGER AUTO_INCREMENT PRIMARY KEY,
    domain VARCHAR(128) NOT NULL,
    host VARCHAR(128) NOT NULL,


    -- datetime this was inserted
    inserted DATETIME NOT NULL, 

    source INT NOT NULL REFERENCES sources(id) ,
    type INT NOT NULL REFERENCES types(id),

    -- EDN of date -> count
    timeline MEDIUMBLOB
);

CREATE INDEX domain_timelines_domain_source_type ON referrer_domain_timelines (domain, host, source, type);

-- Storage of entire timeline per referrer subdomain.
CREATE TABLE referrer_subdomain_timelines (
    id  INTEGER AUTO_INCREMENT PRIMARY KEY,
    domain VARCHAR(128) NOT NULL,
    host VARCHAR(128) NOT NULL,


    -- datetime this was inserted
    inserted DATETIME NOT NULL, 

    source INT NOT NULL REFERENCES sources(id) ,
    type INT NOT NULL REFERENCES types(id),

    -- EDN of date -> count
    timeline MEDIUMBLOB
);

CREATE UNIQUE INDEX domain_timelines_unique ON referrer_subdomain_timelines (domain, host, source, type);

create table referrer_domain_events (
   event DATETIME NULL, 
   count INTEGER NOT NULL,
   domain VARCHAR(128) NOT NULL,
   source INT NOT NULL REFERENCES sources(id) ,
   type INT NOT NULL REFERENCES types(id),
   inserted DATETIME NOT NULL
);

CREATE UNIQUE INDEX referrer_domain_events_unique ON referrer_domain_events (domain, source, type);

create table referrer_subdomain_events (
   event DATETIME NULL, 
   count INTEGER NOT NULL,
   subdomain VARCHAR(128) NOT NULL,
   domain VARCHAR(128) NOT NULL,
   source INT NOT NULL REFERENCES sources(id) ,
   type INT NOT NULL REFERENCES types(id),
   inserted DATETIME NOT NULL
);
CREATE UNIQUE INDEX referrer_subdomain_events_unique on referrer_subdomain_events (subdomain, domain, source, type);

CREATE TABLE doi_domain_referral_timelines (
    id  INTEGER AUTO_INCREMENT PRIMARY KEY,
    doi VARCHAR(700),
    host VARCHAR(128),

    source INT NOT NULL REFERENCES sources(id) ,
    type INT NOT NULL REFERENCES types(id),

    inserted DATETIME NOT NULL, 

    -- EDN of date -> count
    timeline MEDIUMBLOB
) ENGINE = myisam;
CREATE UNIQUE INDEX doi_domain_timelines ON doi_domain_referral_timelines (doi, host, type);
CREATE INDEX doi_domain_timelines_host ON  doi_domain_referral_timelines (host);

CREATE TABLE top_domains (
    id  INTEGER AUTO_INCREMENT PRIMARY KEY,
    month DATETIME NOT NULL,
    domains MEDIUMBLOB
);
CREATE UNIQUE INDEX top_domains_month on top_domains (month);

CREATE TABLE member_domains (
    id  INTEGER AUTO_INCREMENT PRIMARY KEY,
    member_id INTEGER NOT NULL,
    domain VARCHAR(128) NOT NULL
);

CREATE UNIQUE INDEX member_domains_unique ON member_domains(member_id, domain);

CREATE TABLE resolutions (
    doi VARCHAR(700) PRIMARY KEY,
    resolved BOOL default false
);

CREATE TABLE crossmarked_dois (
    doi VARCHAR(700) PRIMARY KEY,
    metadata TEXT
);

CREATE TABLE heartbeat_bucket (
    bucket_date DATETIME NOT NULL,
    type INT NOT NULL REFERENCES types(id),
    heartbeat_count INT DEFAULT 0,
    push_count INT DEFAULT 0
);

CREATE UNIQUE INDEX bucket_date_type ON heartbeat_bucket (bucket_date, type);

-- Example for Wikipedia Cocytus PUSH API. Set real token.
-- insert into tokens (token, allowed_sources, allowed_types) values ("TOKENHERE", "Cocytus", "WikipediaCitation");

insert into sources (ident, name) values ("CrossRefMetadata", "CrossRef Metadata");
insert into sources (ident, name) values ("CrossRefLogs", "CrossRef Resolution Logs");
insert into sources (ident, name) values ("CrossRefRobot", "CrossRef Robot");
insert into sources (ident, name) values ("CrossRefDeposit", "CrossRef Deposit System");
insert into sources (ident, name) values ("Cocytus", "Wikipedia Cocytus");

insert into types (ident, name, milestone, arg1desc) values ("issued", "Publisher Issue date", true, "Date supplied by publisher");
insert into types (ident, name, milestone) values ("deposited","Publisher first deposited with CrossRef", true);
insert into types (ident, name, milestone) values ("updated", "Publisher most recently updated CrossRef metadata", true);
insert into types (ident, name, milestone, arg1desc, arg2desc, arg3desc) values ("first-resolution-test",
                                                                       "First attempt DOI resolution test",
                                                                       true,
                                                                       "Initial resolution URL",
                                                                       "Ultimate resolution URL",
                                                                       "Number of redirect hops");

insert into types (ident, name, milestone, arg1desc, arg2desc, arg3desc) values ("WikipediaCitation",
                                                                        "Citation in Wikipedia",
                                                                        true,
                                                                        "Action",
                                                                        "Page URL",
                                                                        "Timestamp");


insert into types (ident, name, milestone) values ("first-resolution", "First DOI resolution", true);
insert into types (ident, name, milestone) values ("total-resolutions", "Total resolutions count", false);

insert into types (ident, name, milestone) values ("daily-resolutions", "Daily resolutions count", false);
insert into types (ident, name, milestone) values ("monthly-resolutions", "Monthly resolutions count", false);
insert into types (ident, name, milestone) values ("yearly-resolutions", "Yearly resolutions count", false);

insert into types (ident, name, milestone) values ("daily-referral-domain", "Daily referral count from domain", false);
insert into types (ident, name, milestone) values ("monthly-referral-domain", "Monthly referral count from domain", false);
insert into types (ident, name, milestone) values ("yearly-referral-domain", "Yearly referral count from domain", false);
insert into types (ident, name, milestone) values ("total-referrals-domain", "Total referrals count from domain", false);

insert into types (ident, name, milestone) values ("daily-referral-subdomain", "Daily referral count from subdomain", false);
insert into types (ident, name, milestone) values ("monthly-referral-subdomain", "Monthly referral count from subdomain", false);
insert into types (ident, name, milestone) values ("yearly-referral-subdomain", "Yearly referral count from subdomain", false);
insert into types (ident, name, milestone) values ("total-referrals-subdomain", "Total referrals count from subdomain", false);

insert into types (ident, name, milestone, arg1desc) values ("crossmark-update-published", "CrossMark Update to this DOI Published", true, "DOI of update");
