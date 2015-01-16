# DOI Chronograph

Chronological information about DOIs. Information is gathered about DOIs with updates (new or update deposits) since this service started running, 5 December 2014. If a DOI is not found, it may be because it wasn't published / updated since this date.

This can be queried by individual DOI or witha bulk query.

This README is under construction.

Support jwass@crossref.org

## Initial Import

On first run:

To import all metadata from MDAPI:

   lein run import-ever

After this, the daily new-udpates task takes care of this.

To generate the list of member-domains:


    lein run update-member-domains

Once this is run you'll need to restart the webserver to re-load the list. Run this every now and again to keep things up to date.

## Importing Laskuri timelines

The output from a Laskuri batch process is a directory with a number of subdirectories for different types. Make sure this is available locally then run

    lein run import-laskuri «directory»

This will import as many data types are available (you can include only specific types by only including those directories). Importing DOI timelines is by far the longest process. Importing 2 years worth of data took 3196m14.294s (53 hours). 

## Schedule

Import newly updated metadata from MDAPI. Run every day.

    lein run new-updates

Try and resolve newly published DOIs. Run every day.

    lein run resolve

Find an example script at etc/daily.sh.example to run with cron.

## License

Copyright © 2014 CrossRef

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.


## See also

http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
http://www.percona.com/blog/2011/07/09/how-to-change-innodb_log_file_size-safely/
http://www.percona.com/blog/2008/11/21/how-to-calculate-a-good-innodb-log-file-size/
http://dev.mysql.com/doc/refman/5.5/en/innodb-parameters.html#sysvar_innodb_log_buffer_size