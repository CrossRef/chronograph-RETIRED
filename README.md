# DOI Time!

Chronological information about DOIs. Information is gathered about DOIs with updates (new or update deposits) since this service started running, 5 December 2014. If a DOI is not found, it may be because it wasn't published / updated since this date.

This can be queried by individual DOI or witha bulk query.

Support jwass@crossref.org

## Fields:

NB some fields are the 'first X', but are historical, only since this service started running. Marked with an asterisk.

 - `doi` - the DOI
 - `firstResolution` - The initial DOI link redirect.
 - `ultimateResolution` - Where the chain of redirects ultimately ends up
 - `issuedString` - The `issue`, which is approximately the publisher-declared publication date. This is in a special format that can represent dates, or months, or quarters, or years. [See the docs for more info](https://github.com/CrossRef/util#date). 
 - `issuedDate` - The 'issue' transformed, lossily, to a nominal real date.
 - `redepositedDate` - The date on which the metadata was most recently deposited.
 - `firstDepositedDate`* - The date on which the metadata was originally deposited. 
 - `resolved`* - The date on which a resolution was first possible. 

Coming soon

 - `firstResolution`* - The date on which the first known successful DOI resolution (click)

## Usage

### From a browser for a single DOI

    http://148.251.184.90:3000/articles/10.1007/s00003-014-0877-9

### Using curl for a single DOI

HTML

     curl http://148.251.184.90:3000/articles/10.1007/s00003-014-0877-9

CSV

    curl -H "Accept: text/csv" http://148.251.184.90:3000/articles/10.1007/s00003-014-0877-9

JSON

    curl -H "Accept: application/json" http://148.251.184.90:3000/articles/10.1007/s00003-014-0877-9


### Using curl for bulk DOI query

HTML

    curl -H "Accept: text/html" --form upload=@demo-request.txt  http://148.251.184.90:3000/articles/

CSV

    curl -H "Accept: text/csv" --form upload=@demo-request.txt  http://148.251.184.90:3000/articles/

JSON

    curl -H "Accept: application/json" --form upload=@demo-request.txt  http://148.251.184.90:3000/articles/

## License

Copyright © 2014 CrossRef

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
