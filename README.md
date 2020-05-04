# NovichenkoBot ![](https://travis-ci.org/mikeizbicki/NovichenkoBot.png?branch=master)


FIXME: 
On 18 Nov, I noticed that the `USER_AGENT` parameter was incorrectly set to `Mozilla ...` rather than `NovichenkoBot`.  
This likely caused many websites with paywalls to return paywall error messages rather than displaying the content of their site.
This variable was probably set for at least 1-2 weeks, and so many urls likely need to be redownloaded.

FIXME:
At 0100 on Jan 1st, I added more languages to the keywords file.  These languages need to be rescanned for keywords.  Some key langauges like Russian,Chinese,Japanese,Korean are all missing.  We also need to continue to add more keywords.

Things to fix:
1. db system catalog locations
1. more separation of concerns with 
