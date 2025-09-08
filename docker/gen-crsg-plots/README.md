# Generate CSRG Plots cron job

Code used by the monthly cronjob generating the yearly event count plots for the CRSG report.

The plots generated are sent to their [respective section](https://cmsdatapop.web.cern.ch/cmsdatapop/EventCountPlots/) of the [CMSDataPopularity website](https://cmsdatapop.web.cern.ch/cmsdatapop/).

This script will generate two plots, with the following parameters.

|                 | FIG 9                                                        | FIG 10                                                       |
| --------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Tiers           | "GEN" "GEN-SIM" "GEN-RAW" "GEN-SIM-RECO" "AODSIM" "MINIAODSIM" "RAWAODSIM" "NANOAODSIM" "GEN-SIM-DIGI-RAW" "GEN-SIM-RAW" "GEN-SIM-DIGI-RECO" | "RAW" "RECO" "AOD" "RAW-RECO" "USER" "MINIAOD" "NANOAOD"     |
| Skims           | *DEFAULT*                                                    | "PromptReco" "Parking"                                       |
| Remove Patterns | *DEFAULT*                                                    | "test" "backfill" "StoreResults" "monitor" "Error/" "Scouting" "MiniDaq" "/Alca" "L1Accept" "L1EG" "L1Jet" "L1Mu" "PhysicsDST" "VdM" "/Hcal" "express" "Interfill" "Bunnies" "REPLAY" "LogError" |

The data is used in the two ([1](https://monit-grafana.cern.ch/d/CkzjpJwWk/c-rsg-plots?orgId=11&from=1757328010456&to=1757349610456&viewPanel=25) and [2](https://monit-grafana.cern.ch/goto/pmaoXs9HR?orgId=11)) Event Count Plot panels in the [C-RSG Plots Dashboard](https://monit-grafana.cern.ch/goto/ccS1uy9Ng?orgId=11).
