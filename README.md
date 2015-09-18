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
    * semi done
6. implement basic user auth with htaccess
    * Done
7. Testing - user/security#
8. reassess this list
9. make a new airborne-web user
10. Make the page more user guiding
    * prevent submission with no lines
    * better increments of lat long on bounds
    * Interpolation explanation
    * Better titling of bounding box
    * Return the user to webpage if errors are identified on server side
11. Add logging in both the webpage and the processing chain
12. Submission page confirmation email text
13. confirmation email text revisit
14. find out why username only in email box is acceptable
15. make "preset with optimum values" bold
16. scroll box for flightlines table
17. run with scripts not in /users/rsg/stgo
18. no zip ext in some browsers?

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