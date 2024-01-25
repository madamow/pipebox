import os
import time
import configparser as ConfigParser
from pipebox import pipequery
import pandas as pd

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


def use_existing_ticket(jtickets, dict):
    """Looks to see if ticket exists. If it does it will use it instead
       of creating a new subticket.Returns reqnum,jira_id"""

    issues = jtickets[(jtickets['parent_issue'] == dict['parent']) & (jtickets['summary'] == dict['summary'])]
    if issues.shape[0] != 0:
        reqnum = issues.issue_key.item().split('-')[1]
        jira_id = issues.parent_issue.item()
        return (reqnum,jira_id)
    else:
        reqnum, jira_id = create_subticket(jtickets, dict)
        return (reqnum,jira_id)

def get_max_reqnum(jtickets):
    existing = pd.concat([jtickets['parent_issue'], jtickets['issue_key']]).unique().tolist()
    existing.remove('Null')
    existing_reqnums = [int(item.split('-')[-1]) for item in existing]
    return max(existing_reqnums)    


def create_subticket(jtickets, dict):
    """Takes a JIRA connection object and a dictionary and creates a
    subticket. Returns the reqnum,jira_id"""
    
    reqnum = get_max_reqnum(jtickets) + 1
    dict['ticket'] = dict['project']+"-"+str(reqnum)

    #Add entry to the database
    pipeline = pipequery.PipeQuery('db-decade')
    pipeline.add_ticket(dict)

    jira_id = dict['parent']
    return (reqnum,jira_id)


def create_parent_subticket(jtickets, dict, use_existing):
    parent_summary = f"{dict['jira_user']}'s Processing Tickets"
    in_records = jtickets[jtickets['summary']==parent_summary]
    print(in_records)
    dict['parent_summary'] = parent_summary
    if in_records.shape[0] == 0:
        #parent does not exist, create one
        dict['parent'] = f"{dict['project']}-{get_max_reqnum(jtickets) + 1}"
        pipeline = pipequery.PipeQuery('db-decade')
        pipeline.add_ticket(dict, add_parent=True)

    else:
        print(" parent was localized")
        parent = in_records.issue_key.item()
        print(parent)
        dict['parent'] = parent
        
    # Create subticket under specified parent ticket
    if use_existing:
        reqnum,jira_id = use_existing_ticket(jtickets, dict)
        return (reqnum,jira_id)
    else:
        reqnum,jira_id = create_subticket(jtickets, dict)
        return (reqnum,jira_id)


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

    # Check if parent and ticket provided in cmd line are in database. If they are, do nothing
    if parent and ticket:
        pass
    else:
        # get ticket tree from database
        pipeline = pipequery.PipeQuery('db-decade')
        jtickets = pipeline.get_tickets()
        
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
            jira_record = jtickets[jtickets['issue_key']==ticket]
            jira_id = jira_record.parent_issue.item()
        except:
            jira_id = reqnum
        return (reqnum,jira_id)

    if parent and not ticket:
        parent = project + '-' + parent
        args_dict['parent'] = parent

        # Create subticket under specified parent ticket
        if use_existing:
            reqnum,jira_id = use_existing_ticket(jtickets,args_dict)
            return (reqnum,jira_id)
        else:
            reqnum,jira_id = create_subticket(jtickets, args_dict)
            return (reqnum,jira_id)

    if not ticket and not parent:
        reqnum,jira_id = create_parent_subticket(jtickets, args_dict, use_existing)
        return (reqnum,jira_id)
