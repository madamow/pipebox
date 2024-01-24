import os
import time
import configparser as ConfigParser
from pipebox import pipequery


def get_jira_user(section='jira-desdm',services_file=None):
    #May stay with no changes from jira_utils
    Config = ConfigParser.ConfigParser()
    if not services_file:
        services_file = os.path.join(os.environ['HOME'],'.desservices.ini')
    try:
        Config.read(services_file)
        jirauser = Config.get(section,'user')
        return jirauser
    except:
        return os.environ['USER']

def get_reqnum_from_nite(parent,nite):
    """Used for auto-submit for firstcut. If nite exists under parent, grab the reqnum to be used in resubmit_failed."""
    con = get_con('jira-desdm')
    parent = 'DESOPS-' + str(parent)
    issues, count = con.search_for_issue(parent, nite)
    if count != 0:
        reqnum = str(issues[0].key).split('-')[1]
        jira_id = issues[0].fields.parent.key
        return reqnum
    else:
        return None

def use_existing_ticket(con,dict):
    """Looks to see if JIRA ticket exists. If it does it will use it instead
       of creating a new subticket.Returns reqnum,jira_id"""
    issues,count = con.search_for_issue(dict['parent'],dict['summary'])
    if count != 0:
        reqnum = str(issues[0].key).split('-')[1]
        jira_id = issues[0].fields.parent.key
        return (reqnum,jira_id)
    else:
        subticket = str(con.create_jira_subtask(dict['parent'],dict['summary'],
                                                 dict['description'],dict['jira_user']))
        reqnum = subticket.split('-')[1]
        jira_id = dict['parent']
        return (reqnum,jira_id)

def create_subticket(con,dict):
    """Takes a JIRA connection object and a dictionary and creates a 
       subticket. Returns the reqnum,jira_id"""
    subticket = str(con.create_jira_subtask(dict['parent'],dict['summary'],
                                             dict['description'],dict['jira_user']))
    reqnum = subticket.split('-')[1]
    jira_id = dict['parent']
    return (reqnum, jira_id)    

def create_ticket(jira_user, ticket=None, parent=None,
                  summary=None, use_existing=False, project='DESOPS'):
    """ Create a ticket for use in framework processing. If parent is specified, 
    will create a subticket. If ticket is specified, will use that ticket. If no parent 
    or ticket specified, will create a parent and then a subticket. Parent and ticket
    should be specified as the number, e.g., 1515. Returns tuple (reqnum,jira_id):
    (1572,DESOPS-1515)"""
    args_dict = {'jira_user':jira_user,
                 'parent':parent,'ticket':ticket,'summary':summary,
                 'use_existing':use_existing,
                 'project':project}
    
    
    #For coadds - files will go under one reqnum is summary and parent are provided

    # Check if parent and ticket provided in cmd line are in database. If they are, do nothing
    if parent and ticket:
        pass
    else:
        # get data from database
        pipeline = pipequery.PipeQuery('db-decade')

        jtickets = pipeline.get_tickets()
        print(jtickets)
        exit()        
        
    if not summary:
        args_dict['summary'] = "%s's Processing run" % jira_user

    if parent and ticket:
        ticket = project + '-' + ticket
        parent = project + '-' + parent
        args_dict['parent'],args_dict['ticket'] = parent,ticket
        # Return what was given
        return (ticket.split('-')[1],parent)

    if ticket and not parent:
        ticket = project + '-' + ticket
        args_dict['ticket'] = ticket
        reqnum = ticket.split('-')[1] 

        # Use ticket specified and find parent key
        try:
            jira_id = con.get_issue(ticket).fields.parent.key
        except:
            jira_id = reqnum
        return (reqnum,jira_id)

    if parent and not ticket:
        parent = project + '-' + parent
        args_dict['parent'] = parent

        # Create subticket under specified parent ticket
        if use_existing:
            reqnum,jira_id = use_existing_ticket(con,args_dict)
            return (reqnum,jira_id)
        else:
            reqnum,jira_id = create_subticket(con,args_dict)
            return (reqnum,jira_id)
    if not ticket and not parent:
        parent_summary = "%s's Processing Tickets" % jira_user
        parent_description = "Parent ticket for %s's processing tests." % jira_user
        is_parent = con.search_for_parent('DESOPS',parent_summary)
        if is_parent[1] == 0:
            # If no parent ticket found create one
            parent = str(con.create_jira_ticket('DESOPS',parent_summary,parent_description,jira_user))
            args_dict['parent'] = parent
            if use_existing:
                reqnum,jira_id = use_existing_ticket(con,args_dict)
                return (reqnum,jira_id)
            else:
                reqnum,jira_id = create_subticket(con,args_dict)
                return (reqnum,jira_id)
        else:
            # Take found parent and create subticket
            parent = str(is_parent[0][0].key)
            args_dict['parent'] = parent
            if use_existing:
                reqnum,jira_id = use_existing_ticket(con,args_dict)
                return (reqnum,jira_id)
            else:
                reqnum,jira_id = create_subticket(con,args_dict)
                return (reqnum,jira_id)
