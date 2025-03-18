import pandas as pd
from iapt_classes_model import Model
from iapt_classes_global import g

# Class representing a Trial for our simulation - a batch of simulation runs.


class Trial:
    def __init__(self):
        self.df_trial_results = pd.DataFrame()
        self.df_trial_results["Run Number"] = [0]
        self.df_trial_results["Mean Screen Time"] = [0.0]
        self.df_trial_results["Total Referrals Rejected"] = [0]
        self.df_trial_results["Mean Opt-in Wait"] = [0.0]
        self.df_trial_results["Total Opted In"] = [0]
        self.df_trial_results["Mean Q Time TA"] = [0.0]
        self.df_trial_results["Total Accepted"] = [0]
        self.df_trial_results.set_index("Run Number", inplace=True)

        self.asst_weekly_dfs = []
        self.step2_weekly_dfs = []
        self.step3_weekly_dfs = []
        self.step2_waiting_dfs = []
        self.step3_waiting_dfs = []
        self.staff_weekly_dfs = []

    def run_trial(self):

        for run in range(g.number_of_runs):
            my_model = Model(run)
            my_model.run(print_run_results=False)

            self.df_trial_results.loc[run] = [
                my_model.mean_screen_time,
                my_model.reject_ref_total,
                my_model.mean_optin_wait,
                my_model.ref_tot_optin,
                my_model.mean_qtime_ta,
                my_model.tot_ta_accept
            ]

            my_model.step2_results_df = pd.DataFrame(my_model.step2_results_df)
            my_model.step2_sessions_df = pd.DataFrame(
                my_model.step2_sessions_df)
            my_model.step3_results_df = pd.DataFrame(my_model.step3_results_df)
            my_model.step3_sessions_df = pd.DataFrame(
                my_model.step3_sessions_df)

            if run == 0:
                self.step2_results_df = my_model.step2_results_df.copy()
                self.step3_results_df = my_model.step3_results_df.copy()
                self.step2_sessions_df = my_model.step2_sessions_df.copy()
                self.step3_sessions_df = my_model.step3_sessions_df.copy()
            else:
                self.step2_results_df = pd.concat(
                    [self.step2_results_df, my_model.step2_results_df])
                self.step3_results_df = pd.concat(
                    [self.step3_results_df, my_model.step3_results_df])
                self.step2_sessions_df = pd.concat(
                    [self.step2_sessions_df, my_model.step2_sessions_df])
                self.step3_sessions_df = pd.concat(
                    [self.step3_sessions_df, my_model.step3_sessions_df])

            my_model.asst_weekly_stats = pd.DataFrame(
                my_model.asst_weekly_stats)
            my_model.step2_weekly_stats = pd.DataFrame(
                my_model.step2_weekly_stats)
            my_model.step3_weekly_stats = pd.DataFrame(
                my_model.step3_weekly_stats)
            my_model.step2_waiting_stats = pd.DataFrame(
                my_model.step2_waiting_stats)
            my_model.step3_waiting_stats = pd.DataFrame(
                my_model.step3_waiting_stats)
            my_model.staff_weekly_stats = pd.DataFrame(
                my_model.staff_weekly_stats)

            my_model.asst_weekly_stats['Run'] = run
            my_model.step2_waiting_stats['Run'] = run
            my_model.step3_waiting_stats['Run'] = run
            my_model.step2_weekly_stats['Run'] = run
            my_model.step3_weekly_stats['Run'] = run
            my_model.staff_weekly_stats['Run'] = run

            self.asst_weekly_dfs.append(my_model.asst_weekly_stats)
            self.step2_waiting_dfs.append(my_model.step2_waiting_stats)
            self.step3_waiting_dfs.append(my_model.step3_waiting_stats)
            self.step2_weekly_dfs.append(my_model.step2_weekly_stats)
            self.step3_weekly_dfs.append(my_model.step3_weekly_stats)
            self.staff_weekly_dfs.append(my_model.staff_weekly_stats)

            if run == 0:
                self.caseload_weekly_dfs = pd.json_normalize(
                    g.caseload_weekly_stats, 'Data', ['Run Number', 'Week Number'])
            else:
                self.caseload_weekly_dfs = pd.concat([
                    self.caseload_weekly_dfs,
                    pd.json_normalize(g.caseload_weekly_stats, 'Data', [
                                      'Run Number', 'Week Number'])
                ])

        return (
            self.step2_results_df,
            self.step2_sessions_df,
            self.step3_results_df,
            self.step3_sessions_df,
            # self.df_trial_results,
            pd.concat(
                self.asst_weekly_dfs) if self.asst_weekly_dfs else pd.DataFrame(),
            pd.concat(
                self.step2_waiting_dfs) if self.step2_waiting_dfs else pd.DataFrame(),
            pd.concat(
                self.step3_waiting_dfs) if self.step3_waiting_dfs else pd.DataFrame(),
            pd.concat(
                self.staff_weekly_dfs) if self.staff_weekly_dfs else pd.DataFrame(),
            self.caseload_weekly_dfs if hasattr(
                self, 'caseload_weekly_dfs') else pd.DataFrame()
        )
