#!/usr/bin/env python

import os,sys
from PipeBox import replace_fh, get_expnum_info, write_wcl, ALL_CCDS,run_local

def cmdline():

    import argparse
    parser = argparse.ArgumentParser(description="Creates the submit wcl for a EXPNUM\nwrite_sne_wcl_exposure.py SN-X2 229686 1124")
    # The positional arguments
    parser.add_argument("field", action="store",default=None,
                        help="SNe Field")
    parser.add_argument("expnum", action="store",default=None,
                        help="Exposure number")
    parser.add_argument("reqnum", action="store",default=None,
                        help="Request number")
    parser.add_argument("--db_section", action="store", default='db-destest',
                        choices=['db-desoper','db-destest'],
                        help="DB Section to query")
    parser.add_argument("--archive_name", default=None,   
                        help="Archive name (i.e. prodbeta or desar2home)")
    parser.add_argument("--schema", default=None,   
                        help="Schema name (i.e. prodbeta or prod)")
    parser.add_argument("--http_section", default=None,   
                        help="DES Services http-section  (i.e. file-http-prodbeta)")
    parser.add_argument("--target_site", action="store", default='fermigrid-sl6',
                        help="Compute Target Site")
    parser.add_argument("--user", action="store", default=os.environ['USER'],
                        help="username that will submit")
    parser.add_argument("--labels", action="store", default='me-tests',
                        help="Coma-separated labels")
    parser.add_argument("--eups_product", action="store", default='Y2Nstack',
                        help="Name of the EUPS stack to use")
    parser.add_argument("--eups_version", action="store", default='1.0.6+1',
                        help="Name of the EUPS stack to use")
    parser.add_argument("--campaign", action="store", default='Y2T',
                        help="Name of the campaign")
    parser.add_argument("--project", action="store", default='ACT',
                        help="Name of the project ie. ACT/FM/etc")
    parser.add_argument("--libname", action="store", default='Y2N',
                        help="Name of the wcl library to use")
    parser.add_argument("--template", action="store", default='firstcut',
                        help="Name of template to use (without the .des)")
    parser.add_argument("--ccdnums", action="store", default=ALL_CCDS,
                        help="coma-separated list of CCDNUM to use")
    parser.add_argument("--local", action="store_true",default=False,
                        help="run on submit machine without http")
    # For re-runs
    parser.add_argument("--reqnum_input", action="store",default='',
                        help="Input reqnum number for rerun image")
    parser.add_argument("--attnum_input", action="store",default='',
                        help="Input attempt number for rerun image")

    parser.add_argument("--verb", action="store_true", default=False,
                        help="Turn on verbose")
    args = parser.parse_args()

    # Update depending on the db_section
    if not args.archive_name:
        if args.db_section == 'db-desoper': args.archive_name='desar2home'
        if args.db_section == 'db-destest': args.archive_name='prodbeta'

    if not args.schema:
        if args.db_section == 'db-desoper': args.schema='PROD'
        if args.db_section == 'db-destest': args.schema='PRODBETA'

    if not args.http_section:
        if args.db_section == 'db-desoper': args.http_section='file-http-desar2home' # CHECK!!!
        if args.db_section == 'db-destest': args.http_section='file-http-prodbeta'

    return args


if __name__ == "__main__":

    # Get the options
    args  = cmdline()

    try:
        pipebox_work = os.environ['PIPEBOX_WORK']
    except:
        print "must declare $PIPEBOX_WORK"
        sys.exit(1)
    wclnames = []
    single_exposure = True
    # Case 1, multiple expnum in filelist
    if os.path.exists(args.expnum):
        print "# Will read file: %s" % args.expnum
        single_exposure = False
        explist = pd.read_csv(args.expnum)
        explist = explist.fillna('')
        for i in range(len(explist)):
            EXPNUM = str(explist.EXPNUM[i])
            if  'CCDNUMS' in explist.columns:
                if explist.CCDNUMS[i] == '':
                    args.ccdnums   = ALL_CCDS
                else:
                    args.ccdnums   = explist.CCDNUMS[i]
                    
            if 'REQNUM_INPUT' in explist.columns and 'ATTNUM_INPUT' in explist.columns:
                args.reqnum_input = str(explist.REQNUM_INPUT[i])
                args.attnum_input = str(explist.ATTNUM_INPUT[i]) 
           
            wclname = write_wcl(EXPNUM,args)
            wclnames.append(wclname)
            
    # Case 2: single expnum
    else:
        wclname = write_wcl(args.expnum,args)
        wclnames.append(wclname)
    # If --local is specified add necessary orchestration lines to submitwcl
    if args.local:
        run_local(wclname,args.target_site,args.archive_name,args.user)
        print "\n# Please comment out transfer_semname and transfer_stats in {USER}_cfg.des if running locally!\n"

    # Now we write the submit bash file
    if single_exposure :
        submit_name = os.path.join(pipebox_work,'submitme_{FIELD}_{EXPNUM}_{REQNUM}.sh'.format(FIELD=args.field,EXPNUM=args.expnum,REQNUM=args.reqnum))
    else :
        submit_name = os.path.join(pipebox_work,'submitme_{FIELD}_{EXPNUM}_{REQNUM}.sh'.format(EXPNUM=os.path.split(args.expnum)[-1],REQNUM=args.reqnum,FIELD=args.field))
    subm = open(submit_name,'w')
    subm.write("#!/usr/bin/env bash\n\n")
    for wclname in wclnames:
        subm.write("dessubmit %s\nsleep 30\n" % wclname)

    os.chmod(submit_name, 0755)
 
    print "# To submit files:\n"
    print "\t %s\n " % submit_name

    # Print warning of Fermigrid credentials
    if args.target_site == 'fermigrid-sl6':
        print "# For FermiGrid please make sure your credentials are valid"
        print "\t setenv X509_USER_PROXY $HOME/.globus/osg/user.proxy"
        print "\t voms-proxy-info --all"
