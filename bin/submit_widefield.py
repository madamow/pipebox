#!/usr/bin/env python

from pipebox import pipeline

# initialize and get options
widefield = pipeline.WideField()
args = widefield.args

# create JIRA ticket per nite found (default)
if args.exptag:
    widefield.ticket(args,groupby='tag')
elif args.auto:
    widefield.ticket(args)
else:
    if not args.jira_summary:
        args.jira_summary = '_'.join([args.user,args.campaign,
                        args.desstat_pipeline,str(args.submit_time)[:10]])
    widefield.ticket(args,groupby='user')

# write submit files and submit if necessary
# columns should only be values that change per submit (groupby)
widefield.make_templates(columns=['nite','expnum','band'],groupby='expnum')
