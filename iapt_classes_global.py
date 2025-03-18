import pandas as pd

class g:

    # used for testing
    debug_level = 2

    # Referrals
    mean_referrals_pw = 200

    # Screening
    referral_rej_rate = 0.3 # % of referrals rejected, advised 30%
    referral_review_rate = 0.4 # % that go to MDT as prev contact with Trust
    mdt_freq = 2 # how often in weeks MDT takes place, longest time a review referral may wait for review
    review_rej_rate = 0.5 # % ref that go to MDT and get rejected
    base_waiting_list = 2741 # current number of patients on waiting list
    referral_screen_time = 20 # average time it takes to screen one referral by a pwp
    opt_in_wait = 1 # no. of weeks patients have to opt in
    opt_in_qtime = 4 # longest period a patient will wait for tel assessment based on 4 week window for asst slots
    opt_in_rate = 0.75 # % of referrals that opt-in
    asst_6_weeks = 0.9 # % of referrals that are assessed within 6 weeks

    # TA
    ta_time_mins = 60 # time allocated to each TA
    ta_accept_rate = 0.7 ##### assume 70% of TA's accepted, need to check this #####

    # Step 2
    step2_ratio = 0.85 # proportion of patients that go onto Step2 vs Step3
    step2_routes = ['PwP','Group'] # possible Step2 routes
    step2_path_ratios = [0.8,0.2] #[0.94,0.06] # Step2 proportion for each route
    step2_pwp_sessions = 6 # number of PwP sessions at Step2
    step2_pwp_dna_rate = 0.15 # ##### assume 15% DNA rate for PwP
    step2_pwp_1st_mins = 45 # minutes allocated for 1st pwp session
    step2_pwp_fup_mins = 30 # minutes allocated for pwp follow-up session
    step2_session_admin = 15 # number of mins of clinical admin per session
    step2_pwp_period = 16 # max number of weeks PwP delivered over
    step2_group_sessions = 7 # number of group sessions
    step2_group_size = 7 # size a group needs to be before it can be run
    step2_group_session_mins = 240 # minutes allocated to pwp group session
    step2_group_dna_rate = 0.216 # Wellbeing Workshop attendance 78.6%

    # Step Moves
    step2_step3_ratio = [0.85,0.15]
    step_routes = ['Step2','Step3']
    step_up_rate = 0.01 # proportion of Step2 that get stepped up = 0.3% but rounded up to 1%
    step_down_rate = 0.12 # proportion of Step3 that get stepped down = 11.86%

    # Step 3
    step3_ratio = 0.15 # proportion of patients that go onto Step3 vs Step2
    step3_routes =['Couns','CBT'] # full pathway options = ['PfCBT','Group','CBT','EMDR','DepC','DIT','IPT','CDEP']
    step3_path_ratios = [0.368,0.632]# [0.1,0.25,0.25,0.05,0.05,0.1,0.1,0.1] # Step3 proportion for each route ##### Need to clarify exact split
    step3_cbt_sessions = 12 # number of PwP sessions at Step2
    step3_cbt_1st_mins = 45 # minutes allocated for 1st cbt session
    step3_cbt_fup_mins = 30 # minutes allocated for cbt follow-up session
    step3_cbt_dna_rate = 0.216 # Wellbeing Workshop attendance 78.6%
    step3_session_admin = 15 # number of mins of clinical admin per session
    step3_cbt_period = 16 # max number of weeks cbt delivered over
    step3_couns_sessions = 8 # number of couns sessions
    step3_couns_1st_mins = 45 # minutes allocated for 1st couns session
    step3_couns_fup_mins = 30 # minutes allocated for couns follow-up session
    step3_couns_dna_rate = 0.216 # Wellbeing Workshop attendance 78.6%
    step3_couns_period = 16 # max number of weeks couns delivered over
    step_3_session_var = 0.15 # % of instances where number sessions goes over standard amount

    # Staff
    supervision_time = 120 # 2 hours per month per modality ##### could use modulus for every 4th week
    break_time = 100 # 20 mins per day
    wellbeing_time = 120 # 2 hours allocated per month
    counsellors_huddle = 30 # 30 mins p/w or 1 hr per fortnight
    cpd_time = 225 # half day per month CPD

    # Job Plans
    number_staff_cbt = 14 #138
    number_staff_couns = 4 #40
    number_staff_pwp = 12 #125
    hours_avail_cbt = 22.0
    hours_avail_couns = 22.0
    hours_avail_pwp = 21.0
    ta_resource = number_staff_pwp * 3 # job plan = 3 TA per week per PwP
    pwp_resource = number_staff_pwp # starting point for PwP resources
    pwp_caseload = 30
    pwp_id = 0 # unique ID for PwP resources
    pwp_avail = number_staff_pwp # used to check whether a PwP is available
    group_resource = number_staff_pwp #  job plan = 1 group per week per PwP, assume 12 per group
    cbt_resource = number_staff_cbt # job plan = 2 x 1st + 20 X FUP per cbt per week
    cbt_caseload = 25
    cbt_id = 0 # unique ID for CBT resources
    cbt_avail = number_staff_cbt # used to check whether a PwP caseload slot is available
    couns_resource = number_staff_couns # job plan = 2 x 1st + 20 X FUP per cbt per week
    couns_caseload = 25
    couns_id = 0 # unique ID for Couns resources
    couns_avail = number_staff_couns # used to check whether a Couns is available
    dna_policy = 2 # number of DNA's allowed before discharged
    dna_policy_var = 0.05 # % of cases where the DNA policy is varied

    # Simulation
    sim_duration = 52
    number_of_runs = 2
    std_dev = 3 # used for randomising activity times
    event_week_tracker = {} # used to track the latest events week for each patient

    # Result storage
    weekly_wl_posn = pd.DataFrame() # container to hold w/l position at end of week
    caseload_weekly_stats = [] # list to hold weekly Caseload statistics
    number_on_ta_wl = 0 # used to keep track of TA WL position
    number_on_pwp_wl = 0 # used to keep track of PwP WL position
    number_on_group_wl = 0 # used to keep track of groups WL position
    number_on_cbt_wl = 0 # used to keep track of CBT WL position
    number_on_couns_wl = 0 # used to keep track of Couns WL position

    # Caseload
    number_on_pwp_cl = 0 # used to keep track of PwP caseload
    number_on_group_cl = 0 # used to keep track of groups caseload
    number_on_cbt_cl = 0 # used to keep track of CBT caseload
    number_on_couns_cl = 0 # used to keep track of Couns caseload

    # bring in past referral data

    referral_rate_lookup = pd.read_csv('talking_therapies_referral_rates.csv'
                                                               ,index_col=0)
    #print(referral_rate_lookup)
