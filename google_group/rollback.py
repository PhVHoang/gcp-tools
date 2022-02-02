"""
 This is an implementation for rolling back changes if any exception happens when
 executing google_group api.

 Note: Any kind of changes on database need to be done after google groups api calls have been done.

 Proposal:
 - Organize a structured log
 - For every function that calls google group API, it first needs to issue an ID and attach this ID to log
 - When exception occurs on function call -> the rollback apis will be call based on the log ID of this function.
"""
