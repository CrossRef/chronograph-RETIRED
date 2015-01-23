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
CREATE TABLE event_timelines_isam (
    id  INTEGER AUTO_INCREMENT PRIMARY KEY,
    doi VARCHAR(700),

    -- datetime this was inserted
    inserted DATETIME NOT NULL, 

    source INT NOT NULL REFERENCES sources(id) ,
    type INT NOT NULL REFERENCES types(id),

    -- JSON of date -> count
    timeline MEDIUMBLOB
) ENGINE = myisam;

CREATE UNIQUE INDEX event_timelines_doi_source_type_isam ON event_timelines_isam (doi, source, type);
CREATE INDEX event_timelines_doi_isam ON event_timelines_isam (doi);

CREATE TABLE events_isam (
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

    -- if two entries of (doi, source, type) with the same tick are inserted
    -- then they are duplicates. If the tick is unique, then two of the same 
    -- type will be allowed. 
    -- This allows for both kinds of events.
    tick BIGINT NOT NULL DEFAULT 0,

    source INT NOT NULL REFERENCES sources(id) ,
    type INT NOT NULL REFERENCES types(id),

    arg1 TEXT,
    arg2 TEXT,
    arg3 TEXT
) ENGINE = myisam;

CREATE UNIQUE INDEX events_isam_unique on events_isam (doi, source, type, tick);
CREATE INDEX event_doi_source_type_isam on events_isam (doi, source, type);
CREATE INDEX type_event_isam on events_isam (type);
-- For looking up recent events.
CREATE INDEX type_event on events_isam (type, event);
CREATE INDEX event_doi_isam on events_isam (doi);

-- Storage of entire timeline per referrer domain.
CREATE TABLE referrer_domain_timelines (
    id  INTEGER AUTO_INCREMENT PRIMARY KEY,
    domain VARCHAR(128) NOT NULL,
    host VARCHAR(128) NOT NULL,


    -- datetime this was inserted
    inserted DATETIME NOT NULL, 

    source INT NOT NULL REFERENCES sources(id) ,
    type INT NOT NULL REFERENCES types(id),

    -- JSON of date -> count
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

    -- JSON of date -> count
    timeline MEDIUMBLOB
);

CREATE UNIQUE INDEX domain_timelines_unique ON referrer_subdomain_timelines (domain, host, source, type);
CREATE INDEX domain_timelines_subdomain_source_type ON referrer_subdomain_timelines (domain, host, source, type);
CREATE INDEX domain_subdomain ON referrer_subdomain_timelines (domain);

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
    resolved BOOL default false,
    UNIQUE(doi)
);


CREATE TABLE crossmarked_dois (
    doi VARCHAR(700) PRIMARY KEY,
    metadata TEXT
);

CREATE UNIQUE INDEX crossmarked_dois_unique ON crossmarked_dois (doi);

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
