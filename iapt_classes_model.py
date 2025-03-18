import simpy
import pandas as pd
import random
import numpy as np
from utils import vary_number_sessions
from iapt_classes_global import g
from iapt_classes_patient import Patient
from iapt_classes_staff import Staff

class Model:
# Constructor to set up the model for a run. We pass in a run number when
# we create a new model
    def __init__(self, run_number):
        # Create a SimPy environment in which everything will live
        self.env = simpy.Environment()

        # # Create counters for various metrics we want to record
        self.patient_counter = 0
        self.run_number = run_number

        # Create a new DataFrame that will store opt-in results against the patient ID
        self.asst_results_df = pd.DataFrame()
        # Patient
        self.asst_results_df['Patient ID'] = [1]
        # Referral
        self.asst_results_df['Week Number'] = [0]
        self.asst_results_df['Run Number'] = [0]
        self.asst_results_df['At Risk'] = [0] # denotes at risk patient, 1 = Yes, 0 = No
        self.asst_results_df['Referral Time Screen'] = [0.0] # time in mins taken to screen referral
        self.asst_results_df['Referral Rejected'] = [0] # 1 = Yes, 0 = No
        self.asst_results_df['Referral Accepted'] = [0] # 1 = Yes, 0 = No
        self.asst_results_df['Referral Reviewed'] = [0] # 1 = Yes, 0 = No
        self.asst_results_df['Review Wait'] = [0] # time between screening and getting review
        self.asst_results_df['Review Rejected'] = [0] # 1 = Yes, 0 = No
        self.asst_results_df['Opted In'] = [0] # 1 = Yes, 0 = No
        self.asst_results_df['Opt-in Wait'] = [0.0] # time between opt-in notification and patient opting in
        self.asst_results_df['Opt-in Q Time'] = [0.0] # time between opting in and actual TA, 4 week window
        self.asst_results_df['TA Q Time'] = [0] # time spent queueing for TA
        self.asst_results_df['TA WL Posn'] = [0] # position in queue for TA
        self.asst_results_df['TA Outcome'] = [0] # 1 = Accepted, 0 = Rejected
        self.asst_results_df['TA Mins'] = [0] # time allocated to completing TA
        self.asst_results_df['Treatment Path'] = ['NA']

        # Indexing
        self.asst_results_df.set_index("Patient ID", inplace=True)

        self.step2_waiting_list = pd.DataFrame()
        self.step2_waiting_list['Patient ID'] = [1]
        self.step2_waiting_list['Run Number'] = 0
        self.step2_waiting_list['Week Number'] = 0
        self.step2_waiting_list['Route Name'] = ['NA']
        self.step2_waiting_list['IsWaiting'] = 1
        self.step2_waiting_list['WL Position'] = 0
        self.step2_waiting_list['Start Week'] = 0
        self.step2_waiting_list['End Week'] = -1

        self.step2_waiting_list.set_index("Patient ID", inplace=True)

        # Step2
        # Create a new DataFrame that will store opt-in results against the patient ID
        self.step2_results_df = pd.DataFrame()

        self.step2_sessions_df = pd.DataFrame()

        self.step2_results_df['Patient ID'] = [1]
        self.step2_results_df['Week Number'] = [0]
        self.step2_results_df['Run Number'] = [0]
        self.step2_results_df['Route Name'] = ['NA'] # which Step2 pathway the patient was sent down
        self.step2_results_df['Q Time'] = [0.0] # time spent queueing
        self.step2_results_df['WL Posn'] = [0] # place in queue
        self.step2_results_df['IsDropout'] = [0]
        self.step2_results_df['IsStep'] = [0] # was the patent stepped down

        self.step2_sessions_df['Patient ID'] = [1]
        self.step2_sessions_df['Week Number'] = [0]
        self.step2_sessions_df['Run Number'] = [0]
        self.step2_sessions_df['Route Name'] = ['NA'] # which Step2 pathway the patient was sent down
        self.step2_sessions_df['Session Number'] = [0]
        self.step2_sessions_df['Session Time'] = [0] # clinical session time in mins
        self.step2_sessions_df['Admin Time'] = [0] # admin session time in mins
        self.step2_sessions_df['IsDNA'] = [0]

        # Indexing
        self.step2_results_df.set_index("Patient ID", inplace=True)
        self.step2_sessions_df.set_index("Patient ID", inplace=True)

        # Step3
        # Create DataFrames that will store Step3 results against the patient ID

        self.step3_waiting_list = pd.DataFrame()
        self.step3_waiting_list['Patient ID'] = [1]
        self.step3_waiting_list['Route Name'] = ['NA']
        self.step3_waiting_list['IsWaiting'] = 1
        self.step3_waiting_list['WL Position'] = 0
        self.step3_waiting_list['Start Week'] = 0
        self.step3_waiting_list['End Week'] = -1

        self.step3_waiting_list.set_index("Patient ID", inplace=True)

        self.step3_results_df = pd.DataFrame()
        self.step3_sessions_df = pd.DataFrame()

        self.step3_results_df['Patient ID'] = [1]
        self.step3_results_df['Week Number'] = [0]
        self.step3_results_df['Run Number'] = [0]
        self.step3_results_df['Route Name'] = ['NA'] # which Step3 pathway the patient was sent down
        self.step3_results_df['WL Posn'] = [0] # place in queue
        self.step3_results_df['Q Time'] = [0.0] # time spent queueing
        self.step3_results_df['IsDropout'] = [0]
        self.step3_results_df['IsStep'] = [0] # was the patent stepped down

        self.step3_sessions_df['Patient ID'] = [1]
        self.step3_sessions_df['Week Number'] = [0]
        self.step3_sessions_df['Run Number'] = [0]
        self.step3_sessions_df['Route Name'] = ['NA'] # which Step2 pathway the patient was sent down
        self.step3_sessions_df['Session Number'] = [0]
        self.step3_sessions_df['Session Time'] = [0] # clinical session time in mins
        self.step3_sessions_df['Admin Time'] = [0] # admin session time in mins
        self.step3_sessions_df['IsDNA'] = [0]

        # Indexing
        self.step3_results_df.set_index("Patient ID", inplace=True)
        self.step3_sessions_df.set_index("Patient ID", inplace=True)

        # Staff
        # staff counters separated by 100 to ensure no overlap in staff ID's when recording
        self.pwp_staff_counter = 100
        #self.group_staff_counter = 200 # not needed as covered by PwP
        self.cbt_staff_counter = 200
        self.couns_staff_counter = 300

        self.staff_results_df = pd.DataFrame()

        self.staff_results_df['Staff ID'] = [1]
        self.staff_results_df['Week Number'] = [0]
        self.staff_results_df['Run Number'] = [0]
        self.staff_results_df['Job Role'] = ['NA']
        self.staff_results_df['Supervision Mins'] = [0]
        self.staff_results_df['Break Mins'] = [0]
        self.staff_results_df['Wellbeing Mins'] = [0]
        self.staff_results_df['Huddle Mins'] = [0]
        self.staff_results_df['CPD Mins'] = [0]

        self.staff_results_df.set_index("Staff ID", inplace=True)

    # random number generator for activity times
    def random_normal(self, mean, std_dev):
        while True:
            activity_time = random.gauss(mean, std_dev)
            if activity_time > 0:
                return activity_time

    # master process to control the running of all the other processes
    def the_governor(self):

        # start off the governor at week 0
        self.week_number = 0

        # build the caseload resources here, these will be topped up periodically
        yield self.env.process(self.caseload_builder())

        if g.debug_level >= 2:
                    print(f"Building Sim Resources")

        # build the weekly resources needed to run the model
        yield self.env.process(self.resource_builder())

        if g.debug_level >= 2:
                    print(f"Sim Resources Ready")


        if g.debug_level >= 2:
                    print(f"Caseload Resources Ready")

        # list to hold weekly asst statistics
        self.asst_weekly_stats = []
        # list to hold weekly Step2 statistics
        self.step2_weekly_stats = []
        # list to hold weekly Step3 statistics
        self.step3_weekly_stats = []
        # list to hold weekly Staff statistics
        self.staff_weekly_stats = []
        # list to hold Step2 waiting list
        self.step2_waiting_stats = []
        # list to hold Step3 waiting list
        self.step3_waiting_stats = []

        # run for however many times there are weeks in the sim
        while self.week_number < g.sim_duration:

            if g.debug_level >= 2:

                print(f'''
                #################################
                        Week Number {self.week_number}
                #################################
                ''')

            if g.debug_level >= 2:
                    print(f"Topping Up Resources")
            # top up the weekly resources ready for next run
            yield self.env.process(self.replenish_weekly_resources())

            if g.debug_level >= 2:
                    print(f"Topping Up Caseloads")

            # top up the caseload resources ready for next run
            yield self.env.process(self.top_up_caseloads(self.week_number))

            if g.debug_level >= 2:
                    print(f"Firing up the staff entity generator")

            # start up the staff entity generator
            yield self.env.process(self.staff_entity_generator(self.week_number))

            if g.debug_level >= 2:
                    print(f"Staff generator complete")

            if g.debug_level >= 2:
                    print(f"Firing up the referral generator")
            # start up the referral generator
            yield self.env.process(self.generator_patient_referrals(self.week_number))

            if g.debug_level >= 2:
                    print(f"Referral generator has returned {self.referrals_this_week} referrals")

            if g.debug_level >= 2:
                    print(f"Passing {self.referrals_this_week} to the patient treatment generator")

            # start up the patient treatment generator
            yield self.env.process(self.patient_treatment_generator(self.referrals_this_week,self.week_number))

            if g.debug_level >= 2:
                    print(f"Collecting Weekly Stats")

            # collect the weekly stats ready for the next week to run
            yield self.env.process(self.weekly_stats(self.week_number))

            if g.debug_level >= 2:
                    print(f"Weekly Stats have been collected")


            # increment the week number by one now everything has been run for this week
            self.week_number += 1

            if g.debug_level >= 2 and self.week_number<g.sim_duration:
                    print(f"Week {self.week_number-1} complete, moving on to Week {self.week_number}")
            elif g.debug_level >= 2 and self.week_number == g.sim_duration-1:
                print(f"Week {self.week_number-1} complete, simulation has now finished")

            # wait a week before moving onto the next week
            yield self.env.timeout(1)

    def patient_treatment_generator(self,number_of_referrals,treatment_week_number):

        self.referrals_to_process = number_of_referrals
        if g.debug_level >=2:
                print(f'Processing {number_of_referrals} referral through treatment')
        self.treatment_week_number = treatment_week_number

        while self.referrals_to_process > 0:

            if g.debug_level >=2:
                print(f'Processing referral, {self.referrals_to_process} left out of {number_of_referrals}')
            yield self.env.timeout(0)
            self.env.process(self.screen_referral(self.treatment_week_number))

            self.referrals_to_process -= 1

            if g.debug_level >=2:
                print(f'Referral processed, proceeding to next referral, {self.referrals_to_process} left')

        # wait 1 unit of time i.e. 1 week
        #yield self.env.timeout(1)

    # process to capture all the results we need at the end of each week
    def weekly_stats(self,stats_week_number):

            self.stats_week_number = stats_week_number

            ##### record all weekly results #####
            ## Screening & TA
            self.referrals_recvd = self.referrals_this_week
            self.asst_tot_screen = self.asst_results_df['Referral Time Screen'].sum()
            self.asst_tot_accept = self.asst_results_df['Referral Accepted'].sum()
            self.asst_tot_reject = self.asst_results_df['Referral Rejected'].sum()
            self.asst_tot_revwd = self.asst_results_df['Referral Reviewed'].sum()
            self.asst_revw_reject = self.asst_results_df['Review Rejected'].sum()
            self.asst_optin_delay = self.asst_results_df['Opt-in Wait'].mean()
            self.asst_tot_optin = self.asst_results_df['Opted In'].sum()
            self.asst_optin_wait = self.asst_results_df['Opt-in Q Time'].mean()
            self.asst_waiting_list = g.number_on_ta_wl
            self.asst_avg_wait = self.asst_results_df['TA Q Time'].mean()
            self.asst_tot_accept = self.asst_results_df['TA Outcome'].sum()
            self.asst_time_total = self.asst_results_df['TA Mins'].sum()

            self.asst_weekly_stats.append(
                {'Run Number': self.run_number,
                 'Week Number':self.stats_week_number,
                 'Referrals Received':self.referrals_recvd,
                 'Referral Screen Mins':self.asst_tot_screen,
                 'Referrals Accepted':self.asst_tot_accept,
                 'Referrals Rejected':self.asst_tot_reject,
                 'Referrals Reviewed':self.asst_tot_revwd,
                 'Reviews Rejected':self.asst_revw_reject,
                 'Referrals Delay Opt-in':self.asst_optin_delay,
                 'Referrals Opted-in':self.asst_tot_optin,
                 'Referrals Wait Opt-in':self.asst_optin_wait,
                 'TA Waiting List':self.asst_waiting_list,
                 'TA Avg Wait':self.asst_avg_wait,
                 'TA Total Accept':self.asst_tot_accept,
                 'TA Mins':self.asst_time_total
                }
                )



            ## Staff
            self.job_role_list = ['PwP','CBT','Couns']
            # create summary stats for each of the job roles
            for i, job_role in enumerate(self.job_role_list):
                # filter data for appropriate role
                self.staff_results_df_filtered = self.staff_results_df[
                (self.staff_results_df["Job Role"] == job_role) &
                (self.staff_results_df["Week Number"] == self.stats_week_number)
                ]


                self.job_role_name = job_role

                self.staff_tot_superv = self.staff_results_df_filtered['Supervision Mins'].sum()
                self.staff_tot_break = self.staff_results_df_filtered['Break Mins'].sum()
                self.staff_tot_wellb = self.staff_results_df_filtered['Wellbeing Mins'].sum()
                self.staff_tot_huddle = self.staff_results_df_filtered['Huddle Mins'].sum()
                self.staff_tot_cpd = self.staff_results_df_filtered['CPD Mins'].sum()

                # weekly staff non-clinical activity
                self.staff_weekly_stats.append(
                    {'Run Number':self.run_number,
                    'Job Role':self.job_role_name,
                    'Week Number':self.stats_week_number,
                    'Supervision Mins':self.staff_tot_superv,
                    'Break Mins':self.staff_tot_break,
                    'Wellbeing Mins':self.staff_tot_wellb,
                    'Huddle Mins':self.staff_tot_huddle,
                    'CPD Mins':self.staff_tot_cpd
                    }
                    )

            ##### Take a snapshot of the weekly caseload levels #####

            self.weekly_pwp_snapshot = {'Run Number':self.run_number
                                       ,'Week Number':self.stats_week_number
                                       ,'Data':[]}

            for pwp_caseload_id, pwp_level in self.pwp_resources.items():
                self.weekly_pwp_snapshot['Data'].append({
                                            'Caseload ID':pwp_caseload_id
                                            ,'Caseload Level':pwp_level.level})

            g.caseload_weekly_stats.append(self.weekly_pwp_snapshot)

            self.weekly_cbt_snapshot = {'Run Number':self.run_number
                                       ,'Week Number':self.stats_week_number
                                       ,'Data':[]}

            for cbt_caseload_id, cbt_level in self.cbt_resources.items():
                self.weekly_cbt_snapshot['Data'].append({
                                            'Caseload ID':cbt_caseload_id
                                            ,'Caseload Level':cbt_level.level})

            g.caseload_weekly_stats.append(self.weekly_cbt_snapshot)

            self.weekly_couns_snapshot = {'Run Number':self.run_number
                                       ,'Week Number':self.stats_week_number
                                       ,'Data':[]}

            for couns_caseload_id, couns_level in self.couns_resources.items():
                self.weekly_couns_snapshot['Data'].append({
                                            'Caseload ID':couns_caseload_id
                                            ,'Caseload Level':couns_level.level})

            g.caseload_weekly_stats.append(self.weekly_couns_snapshot)

            # record weekly waiting list stats
            ##### Step 2 #####
            self.step2_weekly_waiting_stats = self.step2_waiting_list[self.step2_waiting_list['IsWaiting'] == 1].copy()
            self.step2_weekly_waiting_stats['Weeks Waited'] = self.stats_week_number - self.step2_weekly_waiting_stats['Start Week']

            self.waiting_list_path = ['PwP','Group']
            # create summary stats for each of the routes
            for y, pathway in enumerate(self.waiting_list_path):

                self.step2_weekly_waiting_filtered = self.step2_weekly_waiting_stats[self.step2_weekly_waiting_stats['Route Name'] == pathway]

                self.step2_waiting_count = self.step2_weekly_waiting_filtered['IsWaiting'].sum()
                self.step2_waiting_time = self.step2_weekly_waiting_filtered['Weeks Waited'].mean()

                self.step2_waiting_stats.append(
                    {'Run Number': self.run_number,
                    'Week Number':self.stats_week_number,
                    'Route Name':pathway,
                    'Num Waiting':self.step2_waiting_count,
                    'Avg Wait':self.step2_waiting_time
                    }
                    )

                ##### Step 3 #####
                # record weekly waiting list stats
            self.step3_weekly_waiting_stats = self.step3_waiting_list[self.step3_waiting_list['IsWaiting'] == 1].copy()
            self.step3_weekly_waiting_stats['Weeks Waited'] = self.stats_week_number - self.step2_weekly_waiting_stats['Start Week']

            self.waiting_list_path = ['CBT','Couns']
            # create summary stats for each of the routes
            for z, pathway in enumerate(self.waiting_list_path):

                self.step3_weekly_waiting_filtered = self.step3_weekly_waiting_stats[self.step3_weekly_waiting_stats['Route Name'] == pathway]

                self.step3_waiting_count = self.step3_weekly_waiting_filtered['IsWaiting'].sum()
                self.step3_waiting_time = self.step3_weekly_waiting_filtered['Weeks Waited'].mean()

                self.step3_waiting_stats.append(
                    {'Run Number': self.run_number,
                    'Week Number':self.stats_week_number,
                    'Route Name':pathway,
                    'Num Waiting':self.step3_waiting_count,
                    'Avg Wait':self.step3_waiting_time
                    }
                    )

            # hand control back to the governor function
            yield self.env.timeout(0)

    ##### generator function that represents the DES generator for referrals
    def generator_patient_referrals(self,ref_week_number):

        #while ref_week_number < g.sim_duration:
        self.ref_week_number = ref_week_number

        # get the number of referrals that week based on the mean + seasonal variance
        self.referrals_this_week = round(g.mean_referrals_pw +
                                    (g.mean_referrals_pw *
                                    g.referral_rate_lookup.at[
                                    self.ref_week_number+1,'PCVar'])) # weeks start at 1

        if g.debug_level >= 1:
            print(f'Week {self.week_number}: {self.referrals_this_week}'
                                                    ' referrals generated')

        yield self.env.timeout(0)

    # this function builds staff resources containing the number of slots on the caseload
    # or the number of weekly appointment slots available
    def resource_builder(self):

        ########## Weekly Resources ##########
        ##### TA #####
        self.ta_res = simpy.Container(
            self.env,capacity=g.ta_resource,
            init=g.ta_resource
            )
        ##### Group #####
        self.group_res = simpy.Container(
            self.env,
            capacity=g.group_resource,
            init=g.group_resource
            )

        yield(self.env.timeout(0))

    def caseload_builder(self):

        ##### PwP #####
        self.p_type = 'PwP'
        # dictionary to hold PwP caseload resources
        self.pwp_resources = {f'{self.p_type}_{p}':simpy.Container(self.env,
                    capacity=g.pwp_caseload,
                    init=g.pwp_caseload) for p in range(g.number_staff_pwp)}

        self.weekly_usage = {week: {"week_number": 0, "res_topup": 0} for week in range(g.sim_duration)}

        # dictionary to keep track of resources to be restored
        self.pwp_restore = {f'{self.p_type}_{p}':{} for p in range(g.number_staff_pwp)}

        if g.debug_level >= 2:
            print(self.pwp_restore)
        ##### CBT #####
        self.c_type = 'CBT'
        # dictionary to hold CBT caseload resources
        self.cbt_resources = {f'{self.c_type}_{c}':simpy.Container(self.env,
                    capacity=g.cbt_caseload,
                    init=g.cbt_caseload) for c in range(g.number_staff_cbt)}

        # dictionary to keep track of resources to be restored
        self.cbt_restore = {f'{self.c_type}_{c}':{} for c in range(g.number_staff_cbt)}

        if g.debug_level >= 2:
            print(self.cbt_restore)
        ##### Couns #####
        self.s_type = 'Couns'
        # dictionary to hold Couns caseload resources
        self.couns_resources = {f'{self.s_type}_{s}':simpy.Container(self.env,
                    capacity=g.couns_caseload,
                    init=g.couns_caseload) for s in range(g.number_staff_couns)}

        # dictionary to keep track of resources to be restored
        self.couns_restore = {f'{self.s_type}_{s}':{} for s in range(g.number_staff_couns)}

        if g.debug_level >= 2:
            print(self.couns_restore)

        yield self.env.timeout(0)

    def record_caseload_use(self,r_type,r_id,week_number):

        self.restore_week = week_number

        # Load in the correct resource use dictionary
        if r_type == 'PwP':
            self.resources_used = self.pwp_restore
        elif r_type == 'CBT':
            self.resources_used = self.cbt_restore
        elif r_type == 'Couns':
            self.resources_used = self.couns_restore

        # if g.debug_level >= 2:
        #     print(f'resource used level was:{self.resources_used[r_id]}')

        if r_id not in self.resources_used:

            if g.debug_level >= 2:
                print(f"Error: {r_id} not found in {r_type} resources.")
            return

        # Ensure the week entry exists
        if self.restore_week not in self.resources_used[r_id]:
            self.resources_used[r_id][self.restore_week] = {"week_number": self.restore_week, "res_topup": 0}

        # Now safely update
        self.resources_used[r_id][self.restore_week]['res_topup'] += 1

        if g.debug_level >= 2:
            print(f"Resource {r_id} usage recorded for week {self.restore_week}: {self.resources_used[r_id][self.restore_week]}")

        yield self.env.timeout(0)

    def replenish_weekly_resources(self):

            ##### TA and Group Resources #####
            ta_amount_to_fill = g.ta_resource - self.ta_res.level
            group_amount_to_fill = g.group_resource - self.group_res.level

            if ta_amount_to_fill > 0:
                if g.debug_level >= 2:
                    print(f"TA Level: {self.ta_res.level}")
                    print(f"Putting in {ta_amount_to_fill}")

                self.ta_res.put(ta_amount_to_fill)

                if g.debug_level >= 2:
                    print(f"New TA Level: {self.ta_res.level}")

            if group_amount_to_fill > 0:
                if g.debug_level >= 2:
                    print(f"Group Level: {self.group_res.level}")
                    print(f"Putting in {group_amount_to_fill}")

                self.group_res.put(group_amount_to_fill)

                if g.debug_level >= 2:
                    print(f"New Group Level: {self.group_res.level}")

            # don't wait, go to the next step
            yield self.env.timeout(0)

    def top_up_caseloads(self,week_number):

             ##### Caseload Resources #####
            self.week_num = week_number

            for self.pwp_restore_id, week_data in self.pwp_restore.items():
                if self.week_num in week_data and week_data[self.week_num]['res_topup'] > 0:
                    self.pwp_topup_value = week_data[self.week_num]['res_topup']
                    if g.debug_level >= 2:
                        print(f"PwP Resource {self.pwp_restore_id} at week {self.week_num}: {self.pwp_resources[self.pwp_restore_id].level}, {self.pwp_topup_value} available to restore")

                    self.pwp_resources[self.pwp_restore_id].put(self.pwp_topup_value)

                if g.debug_level >= 2:
                    print(f"PwP Resource {self.pwp_restore_id} now at: {self.pwp_resources[self.pwp_restore_id].level}")

            for self.cbt_restore_id, week_data in self.cbt_restore.items():
                if self.week_num in week_data and week_data[self.week_num]['res_topup'] > 0:
                    self.cbt_topup_value = week_data[self.week_num]['res_topup']
                    if g.debug_level >= 2:
                        print(f"CBT Resource {self.cbt_restore_id} at week {self.week_num}: {self.cbt_resources[self.cbt_restore_id].level}, {self.cbt_topup_value} available to restore")

                    self.cbt_resources[self.cbt_restore_id].put(self.cbt_topup_value)

                if g.debug_level >= 2:
                    print(f"Couns Resource {self.cbt_restore_id} now at: {self.cbt_resources[self.cbt_restore_id].level}")

            for self.couns_restore_id, week_data in self.couns_restore.items():
                if self.week_num in week_data and week_data[self.week_num]['res_topup'] > 0:
                    self.couns_topup_value = week_data[self.week_num]['res_topup']
                    if g.debug_level >= 2:
                            print(f"PwP Resource {self.couns_restore_id} at week {self.week_num}: {self.couns_resources[self.couns_restore_id].level}, {self.couns_topup_value} available to restore")

                    self.couns_resources[self.couns_restore_id].put(self.couns_topup_value)

                if g.debug_level >= 2:
                    print(f"PwP Resource {self.couns_restore_id} now at: {self.couns_resources[self.couns_restore_id].level}")
            # don't wait, go to the next step
            yield self.env.timeout(0)

    def find_caseload_slot(self,r_type):

        self.r_type = r_type
        # load in the right resource dictionary
        if self.r_type == 'PwP':
            self.resources = self.pwp_resources
        elif self.r_type == 'CBT':
            self.resources = self.cbt_resources
        elif self.r_type == 'Couns':
            self.resources = self.couns_resources

        self.available_caseloads = {k: v for k, v in self.resources.items() if v.level > 0}

        # keep going as long as the simulation is still running
        while self.env.now < g.sim_duration:
            if self.available_caseloads:
                # Randomly select from the non-empty resources
                self.random_caseload_id = random.choice(list(self.available_caseloads.keys()))
                self.selected_resource = self.available_caseloads[self.random_caseload_id]

                if g.debug_level >= 2:
                    print(f'Resource {self.random_caseload_id} with a remaining caseload of {self.selected_resource.level} selected')
                return self.random_caseload_id, self.selected_resource # Return the ID and the container if available
                yield self.env.timeout(0)
            else:
                if g.debug_level >=2:
                    print("No available caseload with spaces available!")
                return None, None

            yield self.env.timeout(1)

    ###### generator for staff to record non-clinical activity #####
    def staff_entity_generator(self, week_number):

        yield self.env.process(self.pwp_staff_generator(week_number))
        # self.env.process(self.group_staff_generator(week_number)) # not needed as covered by pwp
        yield self.env.process(self.cbt_staff_generator(week_number))
        yield self.env.process(self.couns_staff_generator(week_number))

        yield(self.env.timeout(0))

    def pwp_staff_generator(self,week_number):

        self.pwp_counter = 0

        # iterate through the PwP staff accounting for half WTE's
        # counter only increments by 0.5 so in effect each staff member
        # will get processed twice each week
        while self.pwp_counter < g.number_staff_pwp:

            # Increment the staff counter by 0.5
            self.pwp_counter += 0.5

            # Create a new staff member from Staff Class
            s = Staff(self.pwp_staff_counter+(self.pwp_counter*2))

            if g.debug_level >=2:
                print('')
                print(f"==== PwP Staff {s.id} Generated ====")

            self.staff_results_df.at[s.id,'Week Number'] = week_number
            self.staff_results_df.at[s.id,'Run Number'] = self.run_number
            self.staff_results_df.at[s.id,'Job Role'] = 'PwP'
            self.staff_results_df.at[s.id,'Break Mins'] = g.break_time/2
            #self.staff_results_df.at[s.id,'Huddle Mins'] = g.huddle_time # counsellors only

            # monthly staff activities
            if self.week_number % 4 == 0:

                self.staff_results_df.at[s.id,'Supervision Mins'] = g.supervision_time/2
                self.staff_results_df.at[s.id,'Wellbeing Mins'] = g.wellbeing_time/2
                self.staff_results_df.at[s.id,'CPD Mins'] = g.cpd_time/2

        yield(self.env.timeout(0))

        # reset the staff counter back to original level once all staff have been processed
        self.pwp_staff_counter = 100

    def cbt_staff_generator(self,week_number):

        self.cbt_counter = 0

        # iterate through the CBT staff accounting for half WTE's
        # counter only increments by 0.5 so in effect each staff member
        # will get processed twice each week
        while self.cbt_counter < g.number_staff_cbt:

            # Increment the staff counter by 0.5
            self.cbt_counter += 0.5

            # Create a new staff member from Staff Class
            s = Staff(self.cbt_staff_counter+(self.cbt_counter*2))

            if g.debug_level >=2:
                print('')
                print(f"==== CBT Staff {s.id} Generated ====")

            self.staff_results_df.at[s.id,'Week Number'] = week_number
            self.staff_results_df.at[s.id,'Run Number'] = self.run_number
            self.staff_results_df.at[s.id,'Job Role'] = 'CBT'
            self.staff_results_df.at[s.id,'Break Mins'] = g.break_time/2
            #self.staff_results_df.at[s.id,'Huddle Mins'] = g.huddle_time # counsellors only

            # monthly staff activities
            if self.week_number % 4 == 0:

                self.staff_results_df.at[s.id,'Supervision Mins'] = g.supervision_time/2
                self.staff_results_df.at[s.id,'Wellbeing Mins'] = g.wellbeing_time/2
                self.staff_results_df.at[s.id,'CPD Mins'] = g.cpd_time/2

        yield(self.env.timeout(0))

        # reset the staff counter back to original level once all staff have been processed
        self.cbt_staff_counter = 200

    def couns_staff_generator(self,week_number):

        self.couns_counter = 0

        # iterate through the Couns staff accounting for half WTE's
        # counter only increments by 0.5 so in effect each staff member
        # will get processed twice each week
        while self.couns_counter < g.number_staff_couns:

            # Increment the staff counter by 0.5
            self.couns_counter += 0.5

            # Create a new staff member from Staff Class
            s = Staff(self.couns_staff_counter+(self.couns_counter*2))

            if g.debug_level >=2:
                print('')
                print(f"==== Couns Staff {s.id} Generated ====")

            self.staff_results_df.at[s.id,'Week Number'] = week_number
            self.staff_results_df.at[s.id,'Run Number'] = self.run_number
            self.staff_results_df.at[s.id,'Job Role'] = 'Couns'
            self.staff_results_df.at[s.id,'Break Mins'] = g.break_time/2
            self.staff_results_df.at[s.id,'Huddle Mins'] = g.counsellors_huddle/2 # Couns only

            # monthly staff activities
            if self.week_number % 4 == 0:

                self.staff_results_df.at[s.id,'Supervision Mins'] = g.supervision_time/2
                self.staff_results_df.at[s.id,'Wellbeing Mins'] = g.wellbeing_time/2
                self.staff_results_df.at[s.id,'CPD Mins'] = g.cpd_time/2

        yield(self.env.timeout(0))

        # reset the staff counter back to original level once all staff have been processed
        self.couns_staff_counter = 300

    ###### assessment part of the clinical pathway #####
    def screen_referral(self, asst_week_number):

        self.asst_week_number = asst_week_number

        # decide whether the referral was rejected at screening stage
        self.reject_referral = random.uniform(0,1)

        # Increment the patient counter by 1
        self.patient_counter += 1

        # Create a new patient from Patient Class
        p = Patient(self.patient_counter)
        p.week_added = asst_week_number
        if g.debug_level >=2:
                print('')
                print(f"==== Patient {p.id} Generated ====")

        # all referrals get screened
        self.asst_results_df.at[p.id, 'Referral Time Screen'
                                        ] = self.random_normal(
                                        g.referral_screen_time
                                        ,g.std_dev)

        # check whether the referral was a straight reject or not
        if self.reject_referral > g.referral_rej_rate:

             # if this referral is accepted mark as accepted
            self.asst_results_df.at[p.id, 'Run Number'] = self.run_number

            self.asst_results_df.at[p.id, 'Week Number'] = self.asst_week_number

            self.asst_results_df.at[p.id, 'Referral Rejected'] = 0

            self.asst_results_df.at[p.id, 'Referral Accepted'] = 1

            if g.debug_level >=2:
                print(f"Referral Accepted flag set to {self.asst_results_df.at[p.id,'Referral Accepted']} for Patient {p.id}")
            # now review the patient

        else:

             # if this referral is rejected mark as rejected
            self.asst_results_df.at[p.id, 'Run Number'] = self.run_number

            self.asst_results_df.at[p.id, 'Week Number'] = self.asst_week_number

            self.asst_results_df.at[p.id, 'Referral Rejected'] = 1

            self.asst_results_df.at[p.id, 'Referral Accepted'] = 0

            if g.debug_level >=2:
                print(f"Referral Rejected for Patient {p.id}")

        if self.reject_referral > g.referral_rej_rate:
            yield self.env.process(self.review_referral(p))
        else:
            yield self.env.timeout(0)

    def review_referral(self,patient):

        # decide whether the referral needs to go for review if not rejected
        self.requires_review = random.uniform(0,1)
        # decide whether the referral is rejected at review
        self.review_reject = random.uniform(0,1)

        p = patient
        # patient needs to be reviewed
        if self.requires_review > g.referral_review_rate:

            if g.debug_level >=2:
                print(f"Patient {p.id} Sent For Review")
            # patient requires review and was rejected
            if self.review_reject < g.review_rej_rate:

                p.referral_review_rej = 1
                p.referral_req_review = 1

                self.asst_results_df.at[p.id,'Review Rejected'] = p.referral_review_rej

                if g.debug_level >=2:
                    print(f"Patient {p.id} Reviewed and Rejection flag set to {self.asst_results_df.at[p.id,'Review Rejected']}")

                # set flag to show Patient required review
                self.asst_results_df.at[p.id,'Referral Reviewed'] = p.referral_req_review
                # record how long they waited for MDT review between 0 and 2 weeks
                self.asst_results_df.at[p.id,'Review Wait'] = random.uniform(0,
                                                                g.mdt_freq)
                # set flag to show they were accepted and go to opt-in
                self.asst_results_df.at[p.id,'Review Rejected'] = p.referral_review_rej

                if g.debug_level >=2:
                    print(f"Patient {p.id} Rejected at Review")

            else:
                # patient requires review and was accepted
                p.referral_review_rej = 0
                p.referral_req_review = 1
                # set flag to show if Patient required review
                self.asst_results_df.at[p.id, 'Referral Reviewed'] = p.referral_req_review
                # therefore no review wait
                self.asst_results_df.at[p.id, 'Review Wait'] = random.uniform(0,
                                                            g.mdt_freq)
                # set flag to show they were accepted and go to opt-in
                self.asst_results_df.at[p.id, 'Review Rejected'] = p.referral_review_rej

                if g.debug_level >=2:
                    print(f"Patient {p.id} Reviewed and Rejection flag set to {self.asst_results_df.at[p.id,'Review Rejected']}")

                if g.debug_level >=2:
                    print(f"Patient {p.id} Accepted at Review - go to opt-in")

        else:
            # patient didn't require review
            p.referral_review_rej = 0
            p.referral_req_review = 0
            if g.debug_level >=2:
                    print(f"Patient {p.id} Didn't Need Review")
            # patient didn't require reviews
            self.asst_results_df.at[p.id, 'Referral Reviewed'] = p.referral_req_review
            # therefore no review wait
            self.asst_results_df.at[p.id, 'Review Wait'] = 0
            # set flag to show they were accepted and go to opt-in
            self.asst_results_df.at[p.id, 'Review Rejected'] = p.referral_review_rej

            if g.debug_level >=2:
                print(f"Patient {p.id} Did Not Require Review, Rejection flag set to {self.asst_results_df.at[p.id,'Review Rejected']}")


            if g.debug_level >=2:
                print(f"Patient {p.id} did not require a Review - go to opt-in")

        if p.referral_review_rej == 1:
            yield self.env.timeout(0)
        else:
            # go to opt-in
            yield self.env.process(self.patient_opt_in(p))

    def patient_opt_in(self,patient):

        p = patient

        # now we can carry on and decide whether the patient opted-in or not
        self.patient_optedin = random.uniform(0,1)
        # decide whether the Patient is accepted following TA
        self.ta_accepted = random.uniform(0,1)

        if self.patient_optedin > g.opt_in_rate:
            # set flag to show Patient failed to opt-in
            self.asst_results_df.at[p.id, 'Opted In'] = 0
            if g.debug_level >=2:
                print(f"Patient {p.id} Failed to Opt In")
            # therefore didn't wait to opt-in
            self.asst_results_df.at[p.id, 'Opt-in Wait'] = 0
            # and didn't queue for TA appt
            self.asst_results_df.at[p.id, 'Opt-in Q Time'] = 0

            # # can stop here and move on to next patient
            yield self.env.timeout(0)

        else: # self.patient_optedin < g.opt_in_rate:
            # otherwise set flag to show they opted-in
            self.asst_results_df.at[p.id, 'Opted In'] = 1
            if g.debug_level >=2:
                print(f"Patient {p.id} Opted In")
            # record how long they took to opt-in, 1 week window
            self.asst_results_df.at[p.id, 'Opt-in Wait'
                                        ] = random.uniform(0,1)
            # record lag-time between opting in and TA appointment, max 4 week window
            self.asst_results_df.at[p.id, 'Opt-in Q Time'
                                        ] = random.uniform(0,4)

            yield self.env.process(self.telephone_assessment(p))

    def telephone_assessment(self,patient):

            p = patient

            start_q_ta = self.env.now

            g.number_on_ta_wl += 1

            # Record where the patient is on the TA WL
            self.asst_results_df.at[p.id, "TA WL Posn"] = \
                                                g.number_on_ta_wl

            # Request a Triage resource from the container
            if g.debug_level >=2:
                print(f"Patient {p.id} requesting TA resource, current res level {self.ta_res.level}")
            with self.ta_res.get(1) as ta_req:
                yield ta_req

            if g.debug_level >=2:
                print(f"Patient {p.id} allocated TA resource, new res level {self.ta_res.level}")

            # as each patient reaches this stage take them off TA wl
            g.number_on_ta_wl -= 1

            if g.debug_level >= 2:
                print(f'Week {self.env.now}: Patient number {p.id} (added week {p.week_added}) put through TA')

            end_q_ta = self.env.now

            # Calculate how long patient queued for TA
            self.q_time_ta = end_q_ta - start_q_ta
            # Record how long patient queued for TA
            self.asst_results_df.at[p.id, 'TA Q Time'] = self.q_time_ta

            if g.debug_level >=2:
                    print(f'Patient {p.id} waited {self.q_time_ta} for assessment')

            # Now do Telephone Assessment using mean and varying
            self.asst_results_df.at[p.id, 'TA Mins'
                                            ] = int(self.random_normal(
                                            g.ta_time_mins
                                            ,g.std_dev))

            # decide if the patient is accepted following TA
            if self.ta_accepted > g.ta_accept_rate:
                # Patient was rejected at TA stage
                self.asst_results_df.at[p.id, 'TA Outcome'] = 0
                if g.debug_level >=2:
                    print(f"Patient {p.id} Rejected at TA Stage")
                #yield self.env.timeout(0)

                # used to decide whether further parts of the pathway are run or not
                self.ta_accepted = 0
            else:
                # Patient was accepted at TA stage
                self.asst_results_df.at[p.id, 'TA Outcome'] = 1
                if g.debug_level >=2:
                    print(f"Patient {p.id} Accepted at TA Stage")

                # used to decide whether further parts of the pathway are run or not
                self.ta_accepted = 1

                if self.ta_accepted == 1 :

                    # if patient was accepted decide which pathway the patient has been allocated to
                    # Select 2 options based on the given probabilities
                    self.step_options = random.choices(g.step_routes, weights=g.step2_step3_ratio, k=self.referrals_this_week)

                    #print(self.selected_step)
                    self.selected_step = random.choice(self.step_options)

                    if self.selected_step == "Step3":
                        if g.debug_level >=2:
                            print(f"Selected step: **{self.selected_step}**")
                    else:
                        if g.debug_level >=2:
                            print(f"Selected step: {self.selected_step}")

                    self.asst_results_df.at[p.id, 'Treatment Path'] = self.selected_step
                    p.initial_step = self.selected_step

                    if g.debug_level >=2:
                        print(f"-- Pathway Runner Initiated --")

                    # now run the pathway runner
                yield self.env.process(self.pathway_runner(p,p.initial_step))

    def pathway_runner(self, patient, step_chosen):

        p = patient

        if step_chosen == 'Step2':
            # record that they've started waiting for treatment
            p.step2_wait_week = self.env.now

            if g.debug_level >=2:
                print(f'PATHWAY RUNNER: Patient {p.id} sent down **{p.initial_step}** pathway')
            #yield self.env.timeout(0)
            yield self.env.process(self.patient_step2_pathway(p))
        else:
            # record that they've started waiting for treatment
            p.step3_wait_week = self.env.now
            if g.debug_level >=2:
                print(f'PATHWAY RUNNER: Patient {p.id} sent down {p.initial_step} pathway')
            #yield self.env.timeout(0)
            yield self.env.process(self.patient_step3_pathway(p))

    ###### step2 pathway #####
    def patient_step2_pathway(self, patient):

        p = patient
        # Select one of 2 treatment options based on the given probabilities
        self.step2_pathway_options = random.choices(g.step2_routes,
                                                weights=g.step2_path_ratios,
                                                k=self.referrals_this_week)

        self.selected_step2_pathway = random.choice(self.step2_pathway_options)

        p.step2_path_route = self.selected_step2_pathway

        self.step2_results_df.at[p.id, 'Route Name'
                                            ] = p.step2_path_route

        # push the patient down the chosen step2 route
        if p.step2_path_route == 'PwP':
            # add to PwP WL
            g.number_on_pwp_wl += 1

            self.step2_waiting_list.at[p.id, 'Route Name'] = p.step2_path_route
            self.step2_waiting_list.at[p.id, 'Run Number'] = self.run_number
            self.step2_waiting_list.at[p.id, 'Week Number'] = self.week_number
            self.step2_waiting_list.at[p.id, 'IsWaiting'] = 1
            self.step2_waiting_list.at[p.id, 'WL Position'] = g.number_on_pwp_wl
            self.step2_waiting_list.at[p.id, 'Start Week'] = self.week_number

        #     self.step2_waiting_list['IsWaiting'] = 1
        # self.step2_waiting_list['WL Position'] = 0
        # self.step2_waiting_list['Start Week'] = 0


            #yield self.env.timeout(0)
            yield self.env.process(self.step2_pwp_process(p))
        else:
            if g.debug_level >=2:
                print(f"Patient {p.id} sent to Group store")

                self.group_store.put(p)

                # add to group WL
                g.number_on_group_wl += 1

                if g.debug_level >=2:
                    print(f'Group store contains {len(self.group_store.items)} of possible {g.step2_group_size}')

                self.start_q_group = self.env.now

                self.step2_waiting_list.at[p.id, 'Route Name'] = p.step2_path_route
                self.step2_waiting_list.at[p.id, 'Run Number'] = self.run_number
                self.step2_waiting_list.at[p.id, 'Week Number'] = self.week_number
                self.step2_waiting_list.at[p.id, 'IsWaiting'] = 1
                self.step2_waiting_list.at[p.id, 'WL Position'] = g.number_on_pwp_wl
                self.step2_waiting_list.at[p.id, 'Start Week'] = self.week_number

                if len(self.group_store.items) == 7:
                    if g.debug_level >=2:
                        print(f'Group is now full, putting {len(self.group_store.items)} through group therapy')
                    # put all the stored patients through group therapy
                    while len(self.group_store.items) > 0:

                        p = yield self.group_store.get()

                        if g.debug_level >=2:
                            print(f'Putting Patient {p.id} through Group Therapy, {len(self.group_store.items)} remaining')
                        if g.debug_level >=2:
                                print(f"FUNC PROCESS patient_step2_pathway: Patient {p.id} Initiating {p.step2_path_route} Step 2 Route")
                        #yield self.env.timeout(0)
                        yield self.env.process(self.step2_group_process(p))

        #yield self.env.timeout(0)

    ###### step3 pathway #####
    def patient_step3_pathway(self, patient):

        p = patient
        # Select one of 2 treatment options based on the given probabilities
        self.step3_pathway_options = random.choices(g.step3_routes,
                                                weights=g.step3_path_ratios,
                                                k=self.referrals_this_week)

        self.selected_step3_pathway = random.choice(self.step3_pathway_options)

        p.step3_path_route = self.selected_step3_pathway

        self.step3_results_df.at[p.id, 'Route Name'
                                            ] = p.step3_path_route

        # push the patient down the chosen step2 route
        if p.step3_path_route == 'CBT':
            # add to CBT WL
            g.number_on_cbt_wl += 1

            self.step3_waiting_list.at[p.id, 'Route Name'] = p.step3_path_route
            self.step3_waiting_list.at[p.id, 'Run Number'] = self.run_number
            self.step3_waiting_list.at[p.id, 'Week Number'] = self.week_number
            self.step3_waiting_list.at[p.id, 'IsWaiting'] = 1
            self.step3_waiting_list.at[p.id, 'WL Position'] = g.number_on_cbt_wl
            self.step3_waiting_list.at[p.id, 'Start Week'] = self.week_number

            if g.debug_level >=2:
                print(f"FUNC PROCESS patient_step3_pathway: Patient {p.id} Initiating {p.step3_path_route} Step 3 Route")

            yield self.env.process(self.step3_cbt_process(p))
        else:
            # add to Couns WL
            g.number_on_couns_wl += 1

            self.step3_waiting_list.at[p.id, 'Route Name'] = p.step3_path_route
            self.step3_waiting_list.at[p.id, 'Run Number'] = self.run_number
            self.step3_waiting_list.at[p.id, 'Week Number'] = self.week_number
            self.step3_waiting_list.at[p.id, 'IsWaiting'] = 1
            self.step3_waiting_list.at[p.id, 'WL Position'] = g.number_on_couns_wl
            self.step3_waiting_list.at[p.id, 'Start Week'] = self.week_number

            if g.debug_level >=2:
                print(f"FUNC PROCESS patient_step3_pathway: Patient {p.id} Initiating {p.step3_path_route} Step 3 Route")
            #yield self.env.timeout(0)
            yield self.env.process(self.step3_couns_process(p))
        #yield self.env.timeout(0)

    def step2_pwp_process(self,patient):

        p = patient

        # counter for number of group sessions
        self.pwp_session_counter = 0
        # counter for applying DNA policy
        self.pwp_dna_counter = 0

        if g.debug_level >=2:
            print(f'{p.step2_path_route} RUNNER: Patient {p.id} added to {p.step2_path_route} waiting list')

        self.start_q_pwp = self.env.now

        # Record where the patient is on the TA WL
        self.step2_results_df.at[p.id, 'WL Posn'] = \
                                            g.number_on_pwp_wl

        if g.debug_level >=2:
            print(f'Patient sent down {p.step2_path_route}')

        while True:
            self.result = yield self.env.process(self.find_caseload_slot(p.step2_path_route))

            if self.result and isinstance(self.result, tuple) and len(self.result) == 2:
                self.pwp_caseload_id, self.pwp_caseload_res = self.result
                if self.pwp_caseload_res is not None:  # Ensure the resource is valid
                    break  # Exit the loop when a resource is found
            else:
                if g.debug_level >= 2:
                    print("No available resource found for PwP, retrying...")
            yield self.env.timeout(1)  # Wait a week and retry

            if self.result == (None, None):
                print(f"Stopping retry as no resources are available. Time: {self.env.now}")
                return  # **Exit function entirely**

        with self.pwp_caseload_res.get(1) as self.pwp_req:
            yield self.pwp_req

        # assign the caseload to the patient
        p.step3_resource_id = self.pwp_caseload_id

        if g.debug_level >=2:
            print(f'Resource {self.pwp_caseload_id} with a caseload remaining of {self.pwp_caseload_res.level} allocated to patient {p.id}')

        # # create a variable to store the current level of the caseload for this resource
        # self.pwp_caseload_posn = self.caseload_[f'{self.caseload_id}']
        # # add to this specific caseload
        # self.pwp_caseload_posn +=1

        if g.debug_level >=2:
            print(f'Patient {p.id} added to caseload {p.step2_resource_id} spaces left')

        # add to caseload
        g.number_on_pwp_cl += 1

        # as each patient reaches this stage take them off PwP wl
        g.number_on_pwp_wl -= 1

        p.step2_start_week = self.env.now

        # update waiting list info
        self.step2_waiting_list.at[p.id, 'IsWaiting'] = 0
        self.step2_waiting_list.at[p.id, 'End Week'] = self.env.now

        if g.debug_level >=2:
            print(f'{p.step2_path_route} RUNNER: Patient {p.id} removed from {p.step2_path_route} waiting list')

        if g.debug_level >= 2:
            print(f'FUNC PROCESS step2_pwp_process: Week {self.env.now}: Patient {p.id} (added week {p.week_added}) put through {p.step2_path_route}')

        self.end_q_pwp = self.env.now

        p.step2_start_week = self.env.now

        self.q_time_pwp = p.step2_start_week - p.step2_wait_week
        if g.debug_level >=2:
            print(f'Patient {p.id} WEEK NUMBER {self.env.now} waited {self.q_time_pwp} weeks from {p.step2_wait_week} weeks to {p.step2_start_week} to enter {p.step2_path_route} treatment')

        self.step2_results_df.at[p.id, 'Route Name'] = p.step2_path_route
        self.step2_results_df.at[p.id, 'Run Number'] = self.run_number
        self.step2_results_df.at[p.id, 'Week Number'] = self.week_number
        # Calculate how long patient queued for PwP
        self.step2_results_df.at[p.id, 'Q Time'] = self.q_time_pwp

        # Generate a list of week numbers the patient is going to attend
        self.pwp_random_weeks = random.sample(range(self.week_number+1, self.week_number+g.step2_pwp_period), g.step2_pwp_sessions)

        # Add 1 at the start of the list
        self.pwp_random_weeks.insert(0, p.step2_start_week)

        # Optionally, sort the list to maintain sequential order
        self.pwp_random_weeks.sort()

        if g.debug_level >=2:
            print(f'Random Session week {len(self.pwp_random_weeks)} numbers are {self.pwp_random_weeks}')

        if g.debug_level >=2:
            print(f'Number of sessions is {g.step2_pwp_sessions}')

        # decide whether the DNA policy had been followed or not
        self.vary_dna_policy = random.uniform(0,1)

        if self.vary_dna_policy >= g.dna_policy_var:
            self.dnas_allowed = 2
        else:
            self.dnas_allowed = 3

        while self.pwp_session_counter < g.step2_pwp_sessions and self.pwp_dna_counter < self.dnas_allowed:

            if g.debug_level >= 2:
                print(f'FUNC PROCESS step2_pwp_process: Week {self.env.now}: Patient {p.id} '
                    f'(added week {p.week_added}) on {p.step2_path_route} '
                    f'Session {self.pwp_session_counter} on Week {self.pwp_random_weeks[self.pwp_session_counter]}')

            # Determine whether the session was DNA'd
            self.dna_pwp_session = random.uniform(0, 1)
            is_dna = 1 if self.dna_pwp_session <= g.step2_pwp_dna_rate else 0

            if is_dna:
                self.pwp_dna_counter += 1
                session_time = 0  # No session time if DNA'd
                admin_time = g.step2_session_admin
            else:
                session_time = g.step2_pwp_1st_mins if self.pwp_session_counter == 0 else g.step2_pwp_fup_mins
                admin_time = g.step2_session_admin

            # Determine if the patient is stepped up
            self.step_patient_up = random.uniform(0, 1)
            is_step_up = 1 if self.pwp_session_counter >= g.step2_pwp_sessions - 1 and self.step_patient_up <= g.step_up_rate else 0
            if is_step_up == 1:
                self.step2_results_df.at[p.id, 'IsStep'] = 1
            else:
                self.step2_results_df.at[p.id, 'IsStep'] = 0
            # Determine if the patient dropped out
            is_dropout = 1 if self.pwp_dna_counter >= self.dnas_allowed else 0
            if is_dropout == 1:
                self.step2_results_df.at[p.id, 'IsDropOut'] = 1
            else:
                self.step2_results_df.at[p.id, 'IsDropOut'] = 0
            # Store session results as a dictionary
            new_row = {
                        'Patient ID': p.id,
                        'Week Number': p.step3_start_week + self.pwp_random_weeks[self.pwp_session_counter],
                        'Run Number': self.run_number,
                        'Route Name': p.step2_path_route,
                        'Session Number': self.pwp_session_counter,
                        'Session Time': session_time,
                        'Admin Time': admin_time,
                        'IsDNA': is_dna
                    }

            # Append the session data to the DataFrame
            self.step2_sessions_df = pd.concat([self.step2_sessions_df, pd.DataFrame([new_row])], ignore_index=True)

            # Handle step-up logic
            if is_step_up:
                self.step2_results_df.at[p.id, 'IsStep'] = 1
                self.pwp_session_counter = 0
                self.pwp_dna_counter = 0  # Reset counters for the next step
                p.step3_wait_week = max(self.pwp_random_weeks)
                if g.debug_level >= 2:
                    print(f'### STEPPED UP ###: Patient {p.id} has been stepped up, running Step3 route selector')
                yield self.env.process(self.patient_step3_pathway(p))

            # Handle dropout logic
            if is_dropout:
                self.step2_results_df.at[p.id, 'IsDropOut'] = 1
                if g.debug_level >= 2:
                    print(f'Patient {p.id} dropped out of {p.step2_path_route} treatment')
                p.step2_end_week = p.step2_start_week + self.pwp_random_weeks[self.pwp_session_counter]
                break  # Stop the loop if patient drops out

            # Move to the next session
            self.pwp_session_counter += 1

        if self.pwp_dna_counter >= self.dnas_allowed:

            self.env.process(self.record_caseload_use(p.step2_path_route,self.pwp_caseload_id,self.pwp_random_weeks[self.pwp_session_counter]))

        else:
            # record when the caseload resource can be restored
            self.env.process(self.record_caseload_use(p.step2_path_route,self.pwp_caseload_id,max(self.pwp_random_weeks)))

        # reset counters for pwp sessions
        self.pwp_session_counter = 0
        self.pwp_dna_counter = 0

        # take off caseload
        g.number_on_pwp_cl -=1

        yield self.env.timeout(0)

    def step2_group_process(self,patient):

        p = patient

        # counter for number of group sessions
        self.group_session_counter = 0
        # counter for applying DNA policy
        self.group_dna_counter = 0

        # Record where the patient is on the TA WL
        self.step2_results_df.at[p.id, 'WL Posn'] = \
                                            g.number_on_group_wl

        # Request a Group resource from the container
        with self.group_res.get(1) as group_req:
            yield group_req

        # add to caseload
        g.number_on_group_cl +=1

        # print(f'Patient {p} started PwP')

        # as each patient reaches this stage take them off Group wl
        g.number_on_group_wl -= 1

        p.step2_start_week = self.env.now

        self.step2_waiting_list.at[p.id, 'IsWaiting'] = 0
        self.step2_waiting_list.at[p.id, 'End Week'] = self.env.now

        if g.debug_level >= 2:
            print(f'FUNC PROCESS step2_group_process: Week {self.env.now}: Patient {p.id} (added week {p.week_added}) put through {p.step2_path_route}')

        self.end_q_group = self.env.now

        # Calculate how long patient queued for groups
        self.q_time_group = p.step2_start_week - p.step2_wait_week
        if g.debug_level >=2:
            print(f'Patient {p.id} WEEK NUMBER {self.env.now} waited {self.q_time_group} weeks from {p.step2_wait_week} weeks to {p.step2_start_week} to enter {p.step2_path_route} treatment')

        self.step2_results_df.at[p.id, 'Route Name'] = p.step2_path_route
        self.step2_results_df.at[p.id, 'Run Number'] = self.run_number
        self.step2_results_df.at[p.id, 'Week Number'] = self.week_number
        # Calculate how long patient queued for PwP
        self.step2_results_df.at[p.id, 'Q Time'] = self.q_time_group

         # Generate a list of week numbers the patient is going to attend
        self.group_random_weeks = random.sample(range(self.week_number+1, self.week_number+10), g.step2_group_sessions)

        # Add 1 at the start of the list
        self.group_random_weeks.insert(0, p.step2_start_week)

        # Optionally, sort the list to maintain sequential order
        self.group_random_weeks.sort()

        if g.debug_level >=2:
            print(f'Random Session week {len(self.pwp_random_weeks)} numbers are {self.pwp_random_weeks}')

        if g.debug_level >=2:
            print(f'Number of sessions is {g.step2_pwp_sessions}')

        # decide whether the DNA policy had been followed or not
        self.vary_dna_policy = random.uniform(0,1)

        if self.vary_dna_policy >= g.dna_policy_var:
            self.dnas_allowed = 2
        else:
            self.dnas_allowed = 3

        while self.group_session_counter < g.step2_group_sessions and self.group_dna_counter < self.dnas_allowed:

            if g.debug_level >= 2:
                print(f'FUNC PROCESS step2_group_process: Week {self.env.now}: Patient {p.id} '
                    f'(added week {p.week_added}) on {p.step2_path_route} '
                    f'Session {self.group_session_counter} on Week {self.group_random_weeks[self.group_session_counter]}')

            # Determine whether the session was DNA'd
            self.dna_group_session = random.uniform(0, 1)
            is_dna = 1 if self.dna_group_session <= g.step2_group_dna_rate else 0

            if is_dna:
                self.group_dna_counter += 1
                session_time = 0  # No session time if DNA'd
                admin_time = g.step2_session_admin
            else:
                session_time = int(g.step2_group_session_mins/g.step2_group_size)
                admin_time = g.step2_session_admin

            # Determine if the patient is stepped up
            self.step_patient_up = random.uniform(0, 1)
            is_step_up = 1 if self.group_session_counter >= g.step2_pwp_sessions - 1 and self.step_patient_up <= g.step_up_rate else 0
            if is_step_up == 1:
                self.step2_results_df.at[p.id, 'IsStep'] = 1
            else:
                self.step2_results_df.at[p.id, 'IsStep'] = 0
            # Determine if the patient dropped out
            is_dropout = 1 if self.group_dna_counter >= self.dnas_allowed else 0
            if is_dropout == 1:
                self.step2_results_df.at[p.id, 'IsDropOut'] = 1
            else:
                self.step2_results_df.at[p.id, 'IsDropOut'] = 0

            # Store session results as a dictionary
            new_row = {
                        'Patient ID': p.id,
                        'Week Number': p.step3_start_week + self.group_random_weeks[self.group_session_counter],
                        'Run Number': self.run_number,
                        'Route Name': p.step2_path_route,
                        'Session Number': self.group_session_counter,
                        'Session Time': session_time,
                        'Admin Time': admin_time,
                        'IsDNA': is_dna
                    }

            # Append the session data to the DataFrame
            self.step2_sessions_df = pd.concat([self.step2_sessions_df, pd.DataFrame([new_row])], ignore_index=True)

            # Handle step-up logic
            if is_step_up:
                self.step2_results_df.at[p.id, 'IsStep'] = 1
                self.group_session_counter = 0
                self.group_dna_counter = 0  # Reset counters for the next step
                # record when they statted waiting i.e. at point of step up
                p.step3_wait_week = max(self.group_random_weeks)
                if g.debug_level >= 2:
                    print(f'### STEPPED UP ###: Patient {p.id} has been stepped up, running Step3 route selector')
                yield self.env.process(self.patient_step3_pathway(p))

            # Handle dropout logic
            if is_dropout:
                self.step2_results_df.at[p.id, 'IsDropOut'] = 1
                if g.debug_level >= 2:
                    print(f'Patient {p.id} dropped out of {p.step2_path_route} treatment')
                p.step2_end_week = p.step2_start_week + self.group_random_weeks[self.group_session_counter]
                break  # Stop the loop if patient drops out

            # Move to the next session
            self.group_session_counter += 1

        # reset counters for pwp sessions
        self.group_session_counter = 0
        self.group_dna_counter = 0

        # take off caseload
        g.number_on_group_cl -=1

        yield self.env.timeout(self.group_session_counter)

    def step3_cbt_process(self,patient):

        p = patient

        # counter for number of cbt sessions
        self.cbt_session_counter = 0
        # counter for applying DNA policy
        self.cbt_dna_counter = 0

        p.step3_start_week = self.env.now

        if g.debug_level >=2:
            print(f'{p.step3_path_route} RUNNER: Patient {p.id} added to {p.step3_path_route} waiting list')

        start_q_cbt = self.env.now

        # Record where the patient is on the cbt WL
        self.step3_results_df.at[p.id, 'WL Posn'] = \
                                            g.number_on_cbt_wl

        # Check if there is a caseload slot available and return the resource
        while True:
            self.result = yield self.env.process(self.find_caseload_slot(p.step3_path_route))

            if self.result and isinstance(self.result, tuple) and len(self.result) == 2:
                self.cbt_caseload_id, self.cbt_caseload_res = self.result
                if self.cbt_caseload_res is not None:  # Ensure the resource is valid
                    break  # Exit the loop when a resource is found
            else:
                if g.debug_level >= 2:
                    print("No available resource found for CBT, retrying...")

            if self.result == (None, None):
                print(f"Stopping retry as no resources are available. Time: {self.env.now}")
                return  # **Exit function entirely**

            yield self.env.timeout(1)  # Wait a week and retry

        # assign the caseload to the patient
        p.step3_resource_id = self.cbt_caseload_id

        if g.debug_level >=2:
            print(f'Resource {self.cbt_caseload_id} with a caseload remaining of {self.cbt_caseload_res.level} allocated to patient {p.id}')

        if g.debug_level >=2:
            print(f'Patient {p.id} added to caseload {p.step3_resource_id}, {self.cbt_resources[p.step3_resource_id].level} spaces left')

        # add to overall caseload
        g.number_on_cbt_cl +=1

        # print(f'Patient {p} started CBT')

        # as each patient reaches this stage take them off CBT WL
        g.number_on_cbt_wl -= 1

        if g.debug_level >=2:
            print(f'{p.step3_path_route} RUNNER: Patient {p.id} removed from {p.step3_path_route} waiting list')

        if g.debug_level >= 2:
            print(f'FUNC PROCESS step3_cbt_process: Week {self.env.now}: Patient {p.id} (added week {p.week_added}) put through {p.step3_path_route}')

        end_q_cbt = self.env.now

        p.step3_start_week = self.week_number

        self.step3_waiting_list.at[p.id, 'IsWaiting'] = 0
        self.step3_waiting_list.at[p.id, 'End Week'] = self.env.now

        # Calculate how long patient queued for CBT
        self.q_time_cbt = p.step3_start_week - p.step3_wait_week
        if g.debug_level >=2:
            print(f'Patient {p.id} WEEK NUMBER {self.env.now} waited {self.q_time_cbt} weeks from {p.step3_wait_week} weeks to {p.step3_start_week} to enter {p.step3_path_route} treatment')

        self.step3_results_df.at[p.id, 'Route Name'] = p.step3_path_route
        self.step3_results_df.at[p.id, 'Run Number'] = self.run_number
        self.step3_results_df.at[p.id, 'Week Number'] = self.week_number
        # Calculate how long patient queued for PwP
        self.step3_results_df.at[p.id, 'Q Time'] = self.q_time_cbt

        # decide whether the DNA policy had been followed or not
        self.vary_dna_policy = random.uniform(0,1)

        if self.vary_dna_policy >= g.dna_policy_var:
            self.dnas_allowed = 2
        else:
            self.dnas_allowed = 3

        # decide whether the number of sessions is going to be varied from standard
        self.vary_step3_sessions = random.uniform(0,1)

        self.random_num_sessions = 0

        self.random_num_sessions += vary_number_sessions(13,35)

        if self.vary_step3_sessions >= g.step_3_session_var:
            self.number_cbt_sessions = g.step3_cbt_sessions # use the standard number of sessions
            self.step3_cbt_period = g.step3_cbt_period # use the standard delivery period
        else:
            self.number_cbt_sessions = self.random_num_sessions # use the randomly generated number of sessions
            self.step3_cbt_period = g.step3_cbt_period+(self.random_num_sessions*2)

        # Generate a list of week numbers the patient is going to attend
        self.cbt_random_weeks = random.sample(range(self.week_number+1, self.week_number+(self.step3_cbt_period*2)), self.number_cbt_sessions)
        # Add 1 at the start of the list
        self.cbt_random_weeks.insert(0, p.step3_start_week)

        # sort the list to maintain sequential order
        self.cbt_random_weeks.sort()

        if g.debug_level >=2:
            print(f'Random Session week {len(self.cbt_random_weeks)} numbers are {self.cbt_random_weeks}')

        if g.debug_level >=2:
            print(f'Number of sessions is {self.number_cbt_sessions}')

        # print(self.random_weeks)

        while self.cbt_session_counter < g.step3_cbt_sessions and self.cbt_dna_counter < self.dnas_allowed:

            if g.debug_level >= 2:
                print(f'FUNC PROCESS step3_cbt_process: Week {self.env.now}: Patient {p.id} '
                    f'(added week {p.week_added}) on {p.step3_path_route} '
                    f'Session {self.cbt_session_counter} on Week {self.cbt_random_weeks[self.cbt_session_counter]}')

            # Determine whether the session was DNA'd
            self.dna_cbt_session = random.uniform(0, 1)
            is_dna = 1 if self.dna_cbt_session <= g.step3_cbt_dna_rate else 0

            if is_dna:
                self.cbt_dna_counter += 1
                session_time = 0  # No session time if DNA'd
                admin_time = g.step3_session_admin
            else:
                session_time = g.step3_cbt_1st_mins if self.cbt_session_counter == 0 else g.step3_cbt_fup_mins
                admin_time = g.step3_session_admin

            # Determine if the patient is stepped up
            self.step_patient_down = random.uniform(0, 1)
            is_step_down = 1 if self.cbt_session_counter >= g.step3_cbt_sessions - 1 and self.step_patient_down <= g.step_down_rate else 0
            if is_step_down == 1:
                self.step3_results_df.at[p.id, 'IsStep'] = 1
            else:
                self.step3_results_df.at[p.id, 'IsStep'] = 0
            # Determine if the patient dropped out
            is_dropout = 1 if self.cbt_dna_counter >= self.dnas_allowed else 0
            if is_dropout == 1:
                self.step3_results_df.at[p.id, 'IsDropOut'] = 1
            else:
                self.step3_results_df.at[p.id, 'IsDropOut'] = 0

            # Store session results as a dictionary
            new_row = {
                        'Patient ID': p.id,
                        'Week Number': p.step3_start_week + self.cbt_random_weeks[self.cbt_session_counter],
                        'Run Number': self.run_number,
                        'Route Name': p.step3_path_route,
                        'Session Number': self.cbt_session_counter,
                        'Session Time': session_time,
                        'Admin Time': admin_time,
                        'IsDNA': is_dna
                        }

            # Append the session data to the DataFrame
            self.step3_sessions_df = pd.concat([self.step3_sessions_df, pd.DataFrame([new_row])], ignore_index=True)

            # Handle step-up logic
            if is_step_down:
                self.step3_results_df.at[p.id, 'IsStep'] = 1
                self.cbt_session_counter = 0
                self.cbt_dna_counter = 0  # Reset counters for the next step
                p.step2_wait_week = max(self.cbt_random_weeks)
                if g.debug_level >= 2:
                    print(f'### STEPPED UP ###: Patient {p.id} has been stepped up, running Step3 route selector')
                yield self.env.process(self.patient_step3_pathway(p))

            # Handle dropout logic
            if is_dropout:
                self.step2_results_df.at[p.id, 'IsDropOut'] = 1
                if g.debug_level >= 2:
                    print(f'Patient {p.id} dropped out of {p.step3_path_route} treatment')
                p.step3_end_week = p.step3_start_week + self.cbt_random_weeks[self.cbt_session_counter]
                break  # Stop the loop if patient drops out

            # Move to the next session
            self.cbt_session_counter += 1

        # # remove from this specific caseload
        # self.cbt_caseload_posn -=1

        if self.cbt_dna_counter >= self.dnas_allowed:

            self.env.process(self.record_caseload_use(p.step3_path_route,self.cbt_caseload_id,self.cbt_random_weeks[self.cbt_session_counter]))

        else:
            # record when the caseload resource can be restored
            self.env.process(self.record_caseload_use(p.step3_path_route,self.cbt_caseload_id,max(self.cbt_random_weeks)))

        # reset counters for pwp sessions
        self.cbt_session_counter = 0
        self.cbt_dna_counter = 0

        # take off caseload
        g.number_on_cbt_cl -=1

        yield self.env.timeout(0)

    def step3_couns_process(self,patient):

        p = patient

        # counter for number of couns sessions
        self.couns_session_counter = 0
        # counter for applying DNA policy
        self.couns_dna_counter = 0

        if g.debug_level >=2:
            print(f'{p.step3_path_route} RUNNER: Patient {p.id} added to {p.step3_path_route} waiting list')

        start_q_couns = self.env.now

        # Record where the patient is on the couns WL
        self.step3_results_df.at[p.id, 'WL Posn'] = \
                                            g.number_on_couns_wl

        # Check if there is a caseload slot available and return the resource
        while True:
            self.result = yield self.env.process(self.find_caseload_slot(p.step3_path_route))

            if self.result and isinstance(self.result, tuple) and len(self.result) == 2:
                self.couns_caseload_id, self.couns_caseload_res = self.result
                if self.couns_caseload_res is not None:  # Ensure the resource is valid
                    break  # Exit the loop when a resource is found
            else:
                if g.debug_level >= 2:
                    print("No available resource found for Couns, retrying...")

            if self.result == (None, None):
                print(f"Stopping retry as no resources are available. Time: {self.env.now}")
                return  # **Exit function entirely**

            yield self.env.timeout(1)  # Wait a week and retry

        with self.couns_caseload_res.get(1) as self.couns_req:
            yield self.couns_req

        # assign the caseload to the patient
        p.step3_resource_id = self.couns_caseload_id

        if g.debug_level >=2:
            print(f'Resource {self.couns_caseload_id} with a caseload remaining of {self.couns_caseload_res.level} allocated to patient {p.id}')

        # # create a variable to store the current level of the caseload for this resource
        # self.pwp_caseload_posn = self.caseload_[f'{self.caseload_id}']
        # # add to this specific caseload
        # self.pwp_caseload_posn +=1

        if g.debug_level >=2:
            print(f'Patient {p.id} added to caseload {p.step3_resource_id}, {self.couns_resources[p.step3_resource_id].level} spaces left')

        # add to overall caseload
        g.number_on_couns_cl +=1

        # print(f'Patient {p} started couns')

        # as each patient reaches this stage take them off couns WL
        g.number_on_couns_wl -= 1

        if g.debug_level >=2:
            print(f'{p.step3_path_route} RUNNER: Patient {p.id} removed from {p.step3_path_route} waiting list')

        if g.debug_level >= 2:
            print(f'FUNC PROCESS step3_couns_process: Week {self.env.now}: Patient {p.id} (added week {p.week_added}) put through {p.step3_path_route}')

        end_q_couns = self.env.now

        p.step3_start_week = self.week_number

        self.step3_waiting_list.at[p.id, 'IsWaiting'] = 0
        self.step3_waiting_list.at[p.id, 'End Week'] = self.env.now

        # Calculate how long patient queued for couns
        self.q_time_couns = p.step3_start_week - p.step3_wait_week

        if g.debug_level >=2:
            print(f'Patient {p.id} WEEK NUMBER {self.env.now} waited {self.q_time_couns} weeks from {p.step3_wait_week} weeks to {p.step3_start_week} to enter {p.step3_path_route} treatment')
        self.step3_results_df.at[p.id, 'Route Name'] = p.step3_path_route
        self.step3_results_df.at[p.id, 'Run Number'] = self.run_number
        self.step3_results_df.at[p.id, 'Week Number'] = self.week_number
        # Calculate how long patient queued for PwP
        self.step3_results_df.at[p.id, 'Q Time'] = self.q_time_couns

        # decide whether the DNA policy had been followed or not
        self.vary_dna_policy = random.uniform(0,1)

        if self.vary_dna_policy >= g.dna_policy_var:
            self.dnas_allowed = 2
        else:
            self.dnas_allowed = 3

        # decide whether the number of sessions is going to be varied from standard
        self.vary_step3_sessions = random.uniform(0,1)

        self.random_num_sessions = 0

        self.random_num_sessions += vary_number_sessions(8,22)

        if self.vary_step3_sessions >= g.step_3_session_var:
            self.number_couns_sessions = g.step3_couns_sessions
            self.step3_couns_period = g.step3_couns_period
        else:
            self.number_couns_sessions = self.random_num_sessions
            self.step3_couns_period = g.step3_couns_period+(self.random_num_sessions*2)

        # Generate a list of week numbers the patient is going to attend
        self.couns_random_weeks = random.sample(range(self.week_number+1, self.week_number+(self.step3_couns_period*2)), self.number_couns_sessions)

        # Add 1 at the start of the list
        self.couns_random_weeks.insert(0, p.step3_start_week)

        # sort the list to maintain sequential order
        self.couns_random_weeks.sort()

        if self.couns_session_counter < len(self.couns_random_weeks):
            p.step3_end_week = p.step3_start_week + self.couns_random_weeks[self.couns_session_counter]
        else:
            if g.debug_level >=2:
                print(f"Warning: Index {self.couns_session_counter} out of range for couns_random_weeks.")

        if g.debug_level >=2:
            print(f'Random Session week {len(self.couns_random_weeks)} numbers are {self.couns_random_weeks}')

        if g.debug_level >=2:
            print(f'Number of sessions is {self.number_couns_sessions}')

        # print(self.random_weeks)

        while self.couns_session_counter < g.step3_couns_sessions and self.couns_dna_counter < self.dnas_allowed:

            if g.debug_level >= 2:
                print(f'FUNC PROCESS step3_couns_process: Week {self.env.now}: Patient {p.id} '
                    f'(added week {p.week_added}) on {p.step3_path_route} '
                    f'Session {self.couns_session_counter} on Week {self.couns_random_weeks[self.couns_session_counter]}')

            # Determine whether the session was DNA'd
            self.dna_couns_session = random.uniform(0, 1)
            is_dna = 1 if self.dna_couns_session <= g.step3_couns_dna_rate else 0

            if is_dna:
                self.couns_dna_counter += 1
                session_time = 0  # No session time if DNA'd
                admin_time = g.step3_session_admin
            else:
                session_time = g.step3_couns_1st_mins if self.couns_session_counter == 0 else g.step3_couns_fup_mins
                admin_time = g.step3_session_admin

            # Determine if the patient is stepped up
            self.step_patient_down = random.uniform(0, 1)
            is_step_down = 1 if self.couns_session_counter >= g.step3_couns_sessions - 1 and self.step_patient_down <= g.step_down_rate else 0
            if is_step_down == 1:
                self.step3_results_df.at[p.id, 'IsStep'] = 1
            else:
                self.step3_results_df.at[p.id, 'IsStep'] = 0
            # Determine if the patient dropped out
            is_dropout = 1 if self.couns_dna_counter >= self.dnas_allowed else 0
            if is_dropout == 1:
                self.step3_results_df.at[p.id, 'IsDropOut'] = 1
            else:
                self.step3_results_df.at[p.id, 'IsDropOut'] = 0

            # Store session results as a dictionary
            new_row = {
                        'Patient ID': p.id,
                        'Week Number': p.step3_start_week + self.couns_random_weeks[self.couns_session_counter],
                        'Run Number': self.run_number,
                        'Route Name': p.step3_path_route,
                        'Session Number': self.couns_session_counter,
                        'Session Time': session_time,
                        'Admin Time': admin_time,
                        'IsDNA': is_dna
                    }

            # Append the session data to the DataFrame
            self.step3_sessions_df = pd.concat([self.step3_sessions_df, pd.DataFrame([new_row])], ignore_index=True)

            # Handle step-up logic
            if is_step_down:
                self.step3_results_df.at[p.id, 'IsStep'] = 1
                self.couns_session_counter = 0
                self.couns_dna_counter = 0  # Reset counters for the next step
                p.step2_wait_week = max(self.couns_random_weeks)
                if g.debug_level >= 2:
                    print(f'### STEPPED UP ###: Patient {p.id} has been stepped up, running Step3 route selector')
                yield self.env.process(self.patient_step3_pathway(p))

            # Handle dropout logic
            if is_dropout:
                self.step3_results_df.at[p.id, 'IsDropOut'] = 1
                if g.debug_level >= 2:
                    print(f'Patient {p.id} dropped out of {p.step3_path_route} treatment')
                p.step3_end_week = p.step3_start_week + self.couns_random_weeks[self.couns_session_counter]
                break  # Stop the loop if patient drops out


            # Move to the next session
            self.couns_session_counter += 1

        # # remove from this specific caseload
        # self.couns_caseload_posn -=1

        if self.couns_dna_counter >= self.dnas_allowed:

            self.env.process(self.record_caseload_use(p.step3_path_route,self.couns_caseload_id,self.couns_random_weeks[self.couns_session_counter]))

        else:
            # record when the caseload resource can be restored
            self.env.process(self.record_caseload_use(p.step3_path_route,self.couns_caseload_id,max(self.couns_random_weeks)))

        # reset counters for pwp sessions
        self.couns_session_counter = 0
        self.couns_dna_counter = 0

        # take off caseload
        g.number_on_couns_cl -=1

        yield self.env.timeout(0)

    # This method calculates results over each single run
    def calculate_run_results(self):
        # Take the mean of the queuing times etc.
        self.mean_screen_time = self.asst_results_df['Referral Time Screen'].mean()
        self.reject_ref_total = self.asst_results_df['Referral Rejected'].sum()
        self.mean_optin_wait = self.asst_results_df['Opt-in Wait'].mean()
        self.ref_tot_optin = self.asst_results_df['Opted In'].sum()
        self.mean_qtime_ta =  self.asst_results_df['Opt-in Q Time'].mean()
        self.tot_ta_accept = self.asst_results_df['TA Outcome'].sum()

        # reset waiting lists and caseloads ready for next run
        g.number_on_pwp_wl = 0
        g.number_on_group_wl = 0
        g.number_on_cbt_wl = 0
        g.number_on_couns_wl = 0
        g.number_on_ta_wl = 0
        g.number_on_pwp_cl = 0
        g.number_on_group_cl = 0
        g.number_on_cbt_cl = 0
        g.number_on_couns_cl = 0

    # The run method starts up the DES entity generators, runs the simulation,
    # and in turns calls anything we need to generate results for the run
    def run(self, print_run_results=True):

        # create a simpy store for this run to hold the patients until the group
        # has enough members. This will persist for an entire run has finished
        # and patients will be added to it and taken out of it as groups fill up
        # and are run
        self.group_store = simpy.Store(
                                        self.env,
                                        capacity=g.step2_group_size
                                        )
        #self.pwp_res_list = {}

        # Start up the week to start processing patients
        self.env.process(self.the_governor())

        # Run the model
        self.env.run()

        # Now the simulation run has finished, call the method that calculates
        # run results
        self.calculate_run_results()

        return self.asst_results_df, self.step2_results_df, self.step3_results_df, self.step2_sessions_df, self.step3_sessions_df

        # Print the run number with the patient-level results from this run of
        # the model
        if print_run_results:
            #print(g.weekly_wl_posn)
            print (f"Run Number {self.run_number}")
            print (self.asst_results_df)
