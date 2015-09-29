The web interface for PIs to submit jobs for processing at PML.

This is built in flask python and uses bootstrap to make the css nice.

THE TODO LIST

To get to version 1:

0. (make everything commented and readable)
1. get emails sending
    * Done
2. get email confirmation/config updating working
    * Done
3. get downloads working
    * Done
4. get user input sanitised server side(!!)
    * Semi done, revisiting this
5. give an error page if the project/day does not exist
    * Done
6. implement basic user auth with htaccess
    * Done
7. Testing - user/security
    * Done
8. reassess this list
    * Done
9. make a new airborne-web user
    * Not doing this
10. Make the page more user guiding
    * prevent submission with no lines
        * Done
    * better increments of lat long on bounds
        * Done
    * Interpolation explanation
        * Done
    * Better titling of bounding box
        * Done
    * Return the user to webpage if errors are identified on server side
        * Done (ish)
11. Add logging in both the webpage and the processing chain
    * Done (ish)
12. Submission page confirmation email text
    * Done
13. confirmation email text revisit
    * Done
14. find out why username only in email box is acceptable
    * It isn't accepted any more
15. make "preset with optimum values" bold
    * Done
16. scroll box for flightlines table
    * Done
17. run with scripts not in /users/rsg/stgo
    * Soon
18. no zip ext in some browsers?
    * Should be fixed

add reset buttons

The glorious future:

1. Add maps for bounding box selection
    * show the primary area
    * show a mosaic of the mapped files
2. improve the band selection format, preferably some kind of text box for entry of lists
3. tooltips
4. uploader for dem
    * zip before upload
    * offer an auto submit on upload complete
5. all masking options
6. support atmospheric correction files
