# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.1.1
#   kernelspec:
#     display_name: Python (opioid_paper)
#     language: python
#     name: opioid_paper
# ---

# # Long Term Trends

# ## Load long term trends data

# +
import pandas as pd
import numpy as np
from ebmdatalab import bq

GBQ_PROJECT_ID = "620265099307"

q4 = """-- opioid long term data extraction 
SELECT 
  year,
  l.chem_substance,
  l.Is_LA, 
  l.Is_High_LA,
  sum(itemsper1000) as items_per_1000, 
  sum(quantityper1000) as quantity_per_1000,
  sum(quantityper1000*dose_per_unit*new_ome_multiplier) AS total_ome_per_1000,
  sum(Infl_corr_Cost_per1000) as cost_per_1000
FROM ebmdatalab.helen.trends_from_pca_final_2017 p
INNER JOIN (SELECT distinct drug_name, chem_substance, Is_LA, Is_High_LA, dose_per_unit, new_ome_multiplier FROM ebmdatalab.richard.opioid_converter) l on l.drug_name = p.drug_name

GROUP BY 
  year,
  chem_substance,
  Is_LA, 
  Is_High_LA"""

dfl = bq.cached_read(q4, csv_path="chemical_summary.zip").fillna(0)
dfl.head()
# -

# tidy data
dfl.loc[dfl["Is_High_LA"]!="TRUE","Is_High_LA"] = "Others"
dfl.loc[dfl["Is_High_LA"]=="TRUE","Is_High_LA"] = "High dose"
dfl.drop("Is_LA",axis=1).groupby(["year","Is_High_LA"]).sum()

# ## Summary results tables

# +
# Summary results table by year
tab = dfl.fillna(0)
tab.loc[tab["Is_High_LA"]=="High dose","OME per 1000_High Dose"] = tab["total_ome_per_1000"]
tab.loc[tab["Is_High_LA"]=="High dose","Items per 1000_High Dose"] = tab["items_per_1000"]
tab.loc[tab["Is_High_LA"]=="High dose","Cost per 1000_High Dose"] = tab["cost_per_1000"]

tab.loc[tab["Is_LA"]==True,"OME per 1000_Long Acting"] = tab["total_ome_per_1000"]
tab.loc[tab["Is_LA"]==True,"Items per 1000_Long Acting"] = tab["items_per_1000"]
tab.loc[tab["Is_LA"]==True,"Cost per 1000_Long Acting"] = tab["cost_per_1000"]

tab = tab.fillna(0)
tab = tab.groupby(["year"]).sum().drop(["Is_LA","quantity_per_1000"],axis=1)
tab = tab.rename(columns={"total_ome_per_1000":"OME per 1000_Total","items_per_1000":"Items per 1000_Total","cost_per_1000":"Cost per 1000_Total"})


tab["OME per 1000_% High Dose"] = (100*tab["OME per 1000_High Dose"]/tab["OME per 1000_Long Acting"])
tab["Items per 1000_% High Dose"] = (100*tab["Items per 1000_High Dose"]/tab["Items per 1000_Long Acting"])
tab["Cost per 1000_% High Dose"] = (100*tab["Cost per 1000_High Dose"]/tab["Cost per 1000_Long Acting"])

tab.columns = tab.columns.str.split('_',expand=True)
tab.sort_index(axis=1, ascending=False)

# +
# Summary by chemical substance
tab = dfl.copy().fillna(0)
tab.loc[tab["Is_High_LA"]=="High dose","OME per 1000_High Dose"] = tab["total_ome_per_1000"]
tab.loc[tab["Is_High_LA"]=="High dose","Items per 1000_High Dose"] = tab["items_per_1000"]
tab.loc[tab["Is_High_LA"]=="High dose","Cost per 1000_High Dose"] = tab["cost_per_1000"]

tab.loc[tab["Is_LA"]==True,"OME per 1000_Long Acting"] = tab["total_ome_per_1000"]
tab.loc[tab["Is_LA"]==True,"Items per 1000_Long Acting"] = tab["items_per_1000"]
tab.loc[tab["Is_LA"]==True,"Cost per 1000_Long Acting"] = tab["cost_per_1000"]

tab = tab.fillna(0)
tab = tab.groupby(["chem_substance"]).sum().drop(["year","Is_LA","quantity_per_1000"],axis=1).sort_values(by="total_ome_per_1000",ascending=False)
tab = tab.rename(columns={"total_ome_per_1000":"OME per 1000_Total","items_per_1000":"Items per 1000_Total","cost_per_1000":"Cost per 1000_Total"})


tab["OME per 1000_% High Dose"] = (100*tab["OME per 1000_High Dose"]/tab["OME per 1000_Long Acting"])
tab["Items per 1000_% High Dose"] = (100*tab["Items per 1000_High Dose"]/tab["Items per 1000_Long Acting"])
tab["Cost per 1000_% High Dose"] = (100*tab["Cost per 1000_High Dose"]/tab["Cost per 1000_Long Acting"])

tab.columns = tab.columns.str.split('_',expand=True)
tab.sort_index(axis=1, ascending=False).fillna(0).head()

# +
# Summary by chemical substance and year (OME only)
tab = dfl.fillna(0)
tab.loc[tab["Is_High_LA"]=="High dose","OME per 1000_High Dose"] = tab["total_ome_per_1000"]

tab.loc[tab["Is_LA"]==True,"OME per 1000_Long Acting"] = tab["total_ome_per_1000"]

grp = tab.loc[tab["year"]==2017]
grp = grp[["chem_substance","OME per 1000_Long Acting"]].groupby(["chem_substance"]).sum().fillna(0).sort_values(by="OME per 1000_Long Acting",ascending=False).reset_index()
grp["chemical"] = np.where(grp.index>5,"Other",grp.chem_substance)
grp = grp.drop(["OME per 1000_Long Acting"], axis=1)
tab2 = tab.reset_index().merge(grp,on="chem_substance")
tab2 = tab2[["year","chemical","total_ome_per_1000","OME per 1000_Long Acting","OME per 1000_High Dose"]].fillna(0)

tab2 = tab2.groupby(["year","chemical"]).sum().sort_values(by="total_ome_per_1000",ascending=False)
tab2 = tab2.rename(columns={"total_ome_per_1000":"OME per 1000_Total"})

tab2 = pd.DataFrame(tab2.stack()).reset_index().rename(columns={"level_2": 'measure', 0:"value"})
tab2 = tab2.set_index(["measure","year","chemical"]).unstack().fillna(0)
tab2["Total"] = tab2.sum(axis=1)
tab2
# -

# ## Plot data - all opioids

# +
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

sns.set_style("whitegrid",{'grid.color': '.9'})
sns.set_palette("tab20",n_colors=14)

dft = dfl.groupby("chem_substance")["total_ome_per_1000"].sum().sort_values(ascending=False).reset_index().rename(columns={"total_ome_per_1000":"total_ome_all_years"})
sort_order = pd.Series(dft.chem_substance.drop_duplicates())

grp = pd.DataFrame(sort_order).reset_index()
grp["chemical"] = np.where(grp.index>10,"Other",grp.chem_substance)
grp["other_flag"] = np.where(grp.index>10,0,1)
dfl2 = dfl.merge(grp,on="chem_substance")

dfs = dfl2.groupby(["other_flag","chemical"])["total_ome_per_1000"].sum().sort_values(ascending=False).reset_index().rename(columns={"total_ome_per_1000":"total_ome_all_years"}).sort_values(by=["other_flag","total_ome_all_years"],ascending=False)
sort_order = pd.Series(dfs.chemical)


s=[(0,'(a)  Total prescriptions for all opioid-containing preparations','Prescriptions per 1000 population','items_per_1000'),
   (1,'(b)  Total Oral Morphine Equivalency (mg) for all opioid-containing preparations','Oral Morphine equivalency (mg) per 1000 population',"total_ome_per_1000"),
   (2,'(c)  Total cost of all opioid-containing preparations','Cost per 1000 population (2017 equivalent GBP)','cost_per_1000')]

fig = plt.figure(figsize=(15,20))
   
for i in s:
    ax = plt.subplot(3,1,i[0]+1)  # layout and position of subplot
    if i[0]==0:
        dfp = dfl2.groupby(['year','chemical'])[i[3]].sum().unstack()
        dfp = dfp.reindex(columns=sort_order)
    if i[0]==1:
        dfp = dfl2.groupby(['year','chemical'])[i[3]].sum().unstack()
        dfp = dfp.reindex(columns=sort_order)
    if i[0]==2:
        dfp = dfl2.groupby(['year','chemical'])[i[3]].sum().unstack()
        dfp = dfp.reindex(columns=sort_order)
    dfp.plot(ax=ax, kind='area',  linewidth=0)
    ax.set_xticks(np.arange(1998, 2017, step=1))
    ax.set_xlabel('Year', size="14")
    ax.set_ylabel(i[2], size="14")
    ax.tick_params(labelsize=12)
    ax.set_title(i[1], size="17")
    handles, labels = ax.get_legend_handles_labels()
    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
    ax.legend(reversed(handles), reversed(labels),loc='center left', fontsize="12",bbox_to_anchor=(1, .62))
    
#plt.savefig("opioids_Figure1_revisedv2.pdf", transparent=True, dpi=300)    
plt.show()


# -

# ## Plot data - long acting preparations

# +
la_df = dfl.loc[(dfl['Is_LA'] == True)]

import matplotlib.pyplot as plt
import seaborn as sns

sns.set_style("whitegrid",{'grid.color': '.9'})
sns.set_palette("Set3",n_colors=14)

dft = la_df.groupby("chem_substance")["total_ome_per_1000"].sum().sort_values(ascending=False).reset_index().rename(columns={"total_ome_per_1000":"total_ome_all_years"})
sort_order = pd.Series(dft.chem_substance.drop_duplicates())

grp = pd.DataFrame(sort_order).reset_index()
grp["chemical"] = np.where(grp.index>11,"Other",grp.chem_substance)
dfl2 = la_df.merge(grp,on="chem_substance")

dfs = dfl2.groupby("chemical")["total_ome_per_1000"].sum().sort_values(ascending=False).reset_index().rename(columns={"total_ome_per_1000":"total_ome_all_years"})
sort_order = pd.Series(dfs.chemical)


s=[(0,'(a)  Total prescriptions for long-acting opioid preparations','Prescriptions per 1000 population','items_per_1000'),
   (1,'(b)  Total Oral Morphine Equivalency (mg) for long-acting opioid preparations','Oral Morphine equivalency (mg) per 1000 population',"total_ome_per_1000"),
   (2,'(c)  Total cost of long-acting opioid preparations','Cost per 1000 population (2017 equivalent GBP)','cost_per_1000')]

fig = plt.figure(figsize=(15,20))
   
for i in s:
    ax = plt.subplot(3,1,i[0]+1)  # layout and position of subplot
    if i[0]==0:
        dfp = dfl2.groupby(['year','chemical'])[i[3]].sum().unstack()
        dfp = dfp.reindex(columns=sort_order)
    if i[0]==1:
        dfp = dfl2.groupby(['year','chemical'])[i[3]].sum().unstack()
        dfp = dfp.reindex(columns=sort_order)
    if i[0]==2:
        dfp = dfl2.groupby(['year','chemical'])[i[3]].sum().unstack()
        dfp = dfp.reindex(columns=sort_order)
    dfp.plot(ax=ax, kind='area',  linewidth=0)
    ax.set_xticks(np.arange(1998, 2017, step=1))
    ax.set_xlabel('Year', size="14")
    ax.set_ylabel(i[2], size="14")
    ax.tick_params(labelsize=12)
    ax.set_title(i[1], size="17")
    handles, labels = ax.get_legend_handles_labels()
    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
    ax.legend(reversed(handles), reversed(labels),loc='center left', fontsize="12",bbox_to_anchor=(1, .62))

plt.show()


# -

# ## Plot data - High dose opioids

# +
high_df = dfl.loc[(dfl['Is_High_LA'] == "High dose")]

import matplotlib.pyplot as plt
import seaborn as sns

sns.set_style("whitegrid",{'grid.color': '.9'})
sns.set_palette("Set3",n_colors=14)

dft = high_df.groupby("chem_substance")["total_ome_per_1000"].sum().sort_values(ascending=False).reset_index().rename(columns={"total_ome_per_1000":"total_ome_all_years"})
sort_order = pd.Series(dft.chem_substance.drop_duplicates())

grp = pd.DataFrame(sort_order).reset_index()
grp["chemical"] = np.where(grp.index>11,"Other",grp.chem_substance)
dfl2 = high_df.merge(grp,on="chem_substance")

dfs = dfl2.groupby("chemical")["total_ome_per_1000"].sum().sort_values(ascending=False).reset_index().rename(columns={"total_ome_per_1000":"total_ome_all_years"})

s=[(0,'(a)  Total prescriptions for high-dose, long-acting opioid preparations','Prescriptions per 1000 population','items_per_1000'),
   (1,'(b)  Total Oral Morphine Equivalency (mg) for high-dose, long-acting opioid preparations','Oral Morphine equivalency (mg) per 1000 population',"total_ome_per_1000"),
   (2,'(c)  Total cost of high-dose, long-acting opioid preparations','Cost per 1000 population (2017 equivalent GBP)', 'cost_per_1000')]

fig = plt.figure(figsize=(15,20))
   
for i in s:
    ax = plt.subplot(3,1,i[0]+1)  # layout and position of subplot
    if i[0]==1:
        dfp = dfl2.groupby(['year','chemical'])[i[3]].sum().unstack()
        dfp = dfp.reindex(columns=sort_order)
    if i[0]==0:
        dfp = dfl2.groupby(['year','chemical'])[i[3]].sum().unstack()
        dfp = dfp.reindex(columns=sort_order)
    if i[0]==2:
        dfp = dfl2.groupby(['year','chemical'])[i[3]].sum().unstack()
        dfp = dfp.reindex(columns=sort_order)
    dfp.plot(ax=ax, kind='area',  linewidth=0)
    ax.set_xticks(np.arange(1998, 2017, step=1))
    ax.set_xlabel('Year', size="14")
    ax.set_ylabel(i[2], size="14")
    ax.tick_params(labelsize=12)
    ax.set_title(i[1], size="17")
    handles, labels = ax.get_legend_handles_labels()
    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
    ax.legend(reversed(handles), reversed(labels),loc='center left', fontsize="12",bbox_to_anchor=(1, .62))

#plt.savefig("opioids_Figure2_revised.pdf", transparent=True, dpi=300)   
plt.show()

# -

# ## Plot data - less popular opioids (expand "Other" group)

# +
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_style("whitegrid",{'grid.color': '.9'})
sns.set_palette("Set3",n_colors=14)

dft = dfl.groupby("chem_substance")["total_ome_per_1000"].sum().sort_values(ascending=False).reset_index().rename(columns={"total_ome_per_1000":"total_ome_all_years"})
sort_order = pd.Series(dft.chem_substance.drop_duplicates())

grp = pd.DataFrame(sort_order).reset_index()
grp = grp.loc[grp.index>=11].reset_index()
grp["chemical"] = np.where(grp.index>10,"Others",grp.chem_substance)
grp["other_flag"] = np.where(grp.index>10,0,1)
dfl2 = dfl.merge(grp,on="chem_substance")

dfs = dfl2.groupby(["other_flag","chemical"])["total_ome_per_1000"].sum().sort_values(ascending=False).reset_index().rename(columns={"total_ome_per_1000":"total_ome_all_years"}).sort_values(by=["other_flag","total_ome_all_years"],ascending=False)
sort_order = pd.Series(dfs.chemical)


s=[(0,'(a)  Total prescriptions for opioid-containing preparations','Prescriptions per 1000 population','items_per_1000'),
   (1,'(b)  Total Oral Morphine Equivalency (mg) for opioid-containing preparations','Oral Morphine equivalency (mg) per 1000 population',"total_ome_per_1000"),
   (2,'(c)  Total cost of opioid-containing preparations','Cost per 1000 population (2017 equivalent GBP)','cost_per_1000')]

fig = plt.figure(figsize=(15,20))
   
for i in s:
    ax = plt.subplot(3,1,i[0]+1)  # layout and position of subplot
    if i[0]==0:
        dfp = dfl2.groupby(['year','chemical'])[i[3]].sum().unstack()
        dfp = dfp.reindex(columns=sort_order)
    if i[0]==1:
        dfp = dfl2.groupby(['year','chemical'])[i[3]].sum().unstack()
        dfp = dfp.reindex(columns=sort_order)
    if i[0]==2:
        dfp = dfl2.groupby(['year','chemical'])[i[3]].sum().unstack()
        dfp = dfp.reindex(columns=sort_order)
    dfp.plot(ax=ax, kind='area',  linewidth=0)
    ax.set_xticks(np.arange(1998, 2017, step=1))
    ax.set_xlabel('Year', size="14")
    ax.set_ylabel(i[2], size="14")
    ax.tick_params(labelsize=12)
    ax.set_title(i[1], size="17")
    handles, labels = ax.get_legend_handles_labels()
    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
    ax.legend(reversed(handles), reversed(labels),loc='center left', fontsize="12",bbox_to_anchor=(1, .62))
    

plt.show()

# -

# ## Plot data - low-dose opioids

# +
low_df = dfl.loc[(dfl['Is_LA'] == True)&(dfl['Is_High_LA'] != "High dose")]

import matplotlib.pyplot as plt
import seaborn as sns

sns.set_style("whitegrid",{'grid.color': '.9'})
sns.set_palette("Set3",n_colors=14)

dft = low_df.groupby("chem_substance")["total_ome_per_1000"].sum().sort_values(ascending=False).reset_index().rename(columns={"total_ome_per_1000":"total_ome_all_years"})
sort_order = pd.Series(dft.chem_substance.drop_duplicates())

grp = pd.DataFrame(sort_order).reset_index()
grp["chemical"] = np.where(grp.index>11,"Other",grp.chem_substance)
dfl2 = low_df.merge(grp,on="chem_substance")

dfs = dfl2.groupby("chemical")["total_ome_per_1000"].sum().sort_values(ascending=False).reset_index().rename(columns={"total_ome_per_1000":"total_ome_all_years"})

s=[(0,'(a)  Total prescriptions for non-high-dose, long-acting opioid preparations','Prescriptions per 1000 population','items_per_1000'),
   (1,'(b)  Total Oral Morphine Equivalency (mg) for non-high-dose, long-acting opioid preparations','Oral Morphine equivalency (mg) per 1000 population',"total_ome_per_1000"),
   (2,'(c)  Total cost of non-high-dose, long-acting opioid preparations','Cost per 1000 population (2017 equivalent GBP)', 'cost_per_1000')]

fig = plt.figure(figsize=(15,20))
   
for i in s:
    ax = plt.subplot(3,1,i[0]+1)  # layout and position of subplot
    if i[0]==1:
        dfp = dfl2.groupby(['year','chemical'])[i[3]].sum().unstack()
        dfp = dfp.reindex(columns=sort_order)
    if i[0]==0:
        dfp = dfl2.groupby(['year','chemical'])[i[3]].sum().unstack()
        dfp = dfp.reindex(columns=sort_order)
    if i[0]==2:
        dfp = dfl2.groupby(['year','chemical'])[i[3]].sum().unstack()
        dfp = dfp.reindex(columns=sort_order)
    dfp.plot(ax=ax, kind='area',  linewidth=0)
    ax.set_xticks(np.arange(1998, 2017, step=1))
    ax.set_xlabel('Year', size="14")
    ax.set_ylabel(i[2], size="14")
    ax.tick_params(labelsize=12)
    ax.set_title(i[1], size="17")
    handles, labels = ax.get_legend_handles_labels()
    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
    ax.legend(reversed(handles), reversed(labels),loc='center left', fontsize="12",bbox_to_anchor=(1, .62))

plt.show()

# -

# ## Find first appearances in prescribing data for each opioid (broken down by formulation)

# +

q = '''-- opioid long term data extraction with classification by formulation
WITH a AS (
SELECT 
  year,
  l.chem_substance,
  REGEXP_EXTRACT(l.drug_name, r'[^_]+_(.*?)(?:[0-9]+.*)?$') AS mid_string,
  l.Is_LA, 
  l.Is_High_LA,
  sum(itemsper1000) as items_per_1000, 
  sum(quantityper1000) as quantity_per_1000,
  sum(quantityper1000*dose_per_unit*new_ome_multiplier) AS total_ome_per_1000,
  sum(Infl_corr_Cost_per1000) as cost_per_1000
FROM ebmdatalab.helen.trends_from_pca_final_2017 p
INNER JOIN (SELECT distinct drug_name, chem_substance, Is_LA, Is_High_LA, dose_per_unit, new_ome_multiplier FROM ebmdatalab.richard.opioid_converter) l on l.drug_name = p.drug_name

GROUP BY 
  year,
  chem_substance,
  mid_string,
  Is_LA, 
  Is_High_LA )
  
  
  
  
  SELECT DISTINCT chem_substance, --RTRIM(mid_string) AS mid_string,
  CASE  WHEN RTRIM(mid_string) IN ('Vag Crm','Vag Gel','Oily Crm','Crm','Gel','Lot','Gel','Soln','Gel Sach','Oint','Intrasite Gel') THEN 'Cream/Gel'    --this will only compare identical formulations --  PROBABLY WANT TO INCLUDE EQUIVALENT FORMULATIONS --
        WHEN RTRIM(mid_string) IN ('Tab Buccal','Buccal Film','Tab Subling','Disper Tab','Orodisper Tab','Loz','Tab Sublingual','Oral Lyophilisate') THEN 'Lozenge/Sublingual tab'
        WHEN RTRIM(mid_string) IN ('Tab Solb','Eff Tab','Solb Tab','Tab Eff','Pdr Sach','Eff Pdr Sach','Susp Gran Sach') THEN 'Soluble Tab/Powder'
        WHEN RTRIM(mid_string) LIKE '%Tab' OR RTRIM(mid_string) LIKE '%Cap' OR RTRIM(mid_string) IN ('Cap','Cap E/C','Capl') THEN 'Tab/Cap'
        WHEN RTRIM(mid_string) LIKE '%Susp%' OR
          RTRIM(mid_string) IN ('Liq Spec','Oral Soln','Sod Oral Soln','Elix','Liq','Oral Susp','Tinct','Liq Conc','Oral Dps','Oral Soln Conc','Oral Conc','Mix','Elix BPC Inc Duty','Methadone HCl Mix') THEN 'Liquid'
        WHEN RTRIM(mid_string) LIKE '%Inj' OR RTRIM(mid_string) IN ('I/V Inf','Syr','Inj','(S)','Lact Inj','P','Morph Sulf','Morph Sulph','Implant') THEN 'Injectible'
        WHEN RTRIM(mid_string) IN ('Patch','Patches','TransdermalPatch','Transdermal Patch','T/Derm Patch') THEN 'Patch'
        WHEN RTRIM(mid_string) IN ('Lact Suppos','Suppos') THEN 'Suppository'
        WHEN RTRIM(mid_string) LIKE '%Nsl Spy%' or RTRIM(mid_string) = 'Reefer' THEN 'Inhalation'
        ELSE RTRIM(mid_string) END AS formulation,
        MIN(year) AS min_year
   FROM a
   GROUP BY chem_substance,  formulation
   ORDER BY chem_substance'''

tbl = bq.cached_read(q, csv_path='by_formulation.zip')
tbl.head()
# -

#tbl[["chem_substance","min_year","formulation"]].sort_values(by=["chem_substance","min_year"]).set_index(["chem_substance","min_year"])
tab = pd.DataFrame(tbl.groupby(["chem_substance","min_year"])["formulation"].apply(lambda x:  "%s" % ', '.join(x)))
#tab.to_csv('first_year.csv')
tab

# # Practice and CCG level analysis

# +

#GBQ_PROJECT_ID = '620265099307'


# returns 1.2m rows - saved as 235MB of zipped csvs, loaded in next cell
q = '''
-- opioids - including practices open/prescribing but not prescribing opioids
WITH opioid_prescribing AS (
select p.*
FROM ebmdatalab.hscic.normalised_prescribing_standard p
INNER JOIN (SELECT DISTINCT bnf_code FROM `richard.opioid_converter`) o
ON p.bnf_code = o.bnf_code
),

-- create a table of all prescribing by practice by month.
A AS (
  SELECT practice, pct,
    CAST(month AS DATE) AS year_mon,
    sum(items) AS items
  FROM ebmdatalab.hscic.normalised_prescribing_standard p
  GROUP BY  practice, pct, year_mon),

-- inner join to practice list to filter for type 4 practices, and for months they had more than zero patients and more than zero total prescribing. 
q2 AS (
  SELECT a.practice, 
    a.pct,
    prac.status_code,
    a.year_mon, 
    MAX(a.items) AS items, 
    MAX(total_list_size) AS total_list_size
  FROM ebmdatalab.hscic.practice_statistics_all_years s
  LEFT JOIN A ON  a.practice = s.practice AND a.year_mon = CAST(s.month AS DATE)  
  INNER JOIN  ebmdatalab.hscic.practices prac ON prac.code=a.practice AND prac.setting = 4 
  WHERE total_list_size > 0 and items > 0
  GROUP BY practice, pct, status_code, year_mon )

-- join practices to opioid prescribing data  
SELECT 
  COALESCE(p.pct,q2.pct) AS pct,
  q2.practice,
  q2.status_code,
  q2.year_mon AS month,
  l.chem_substance,
  Is_LA,
  Is_High_LA,
  sum(p.items) as items, 
  sum(quantity) as quantity,
  sum(quantity*dose_per_unit*new_ome_multiplier) AS total_ome,
  sum(net_cost) as net_cost,
  sum(actual_cost) as actual_cost
FROM q2 
LEFT JOIN opioid_prescribing p ON p.practice = q2.practice AND CAST(p.month AS DATE) = q2.year_mon  
LEFT JOIN (SELECT distinct bnf_code, chem_substance, Is_High_LA, Is_LA, dose_per_unit, new_ome_multiplier FROM ebmdatalab.richard.opioid_converter) l on l.bnf_code = p.bnf_code
GROUP BY 
  pct,
  practice,
  status_code,
  month,
  chem_substance,
  Is_LA,
  Is_High_LA
'''



# +
import glob
import pandas as pd
import numpy as np

schema = {
    "pct": "category",
    "practice": "category",
    "status_code": "category",
    "chem_substance": "category",
    "Is_LA": "category",
    "Is_High_LA": "category",
    "items": pd.Int16Dtype(),
    "quantity": pd.Int64Dtype(),
    "total_ome": np.float16,
    "net_cost": np.float16,
    "actual_cost": np.float16,
}


def as_true_false(val):
    "Convert to true/false strings"
    if isinstance(val, str):
        if not val:
            val = None
        elif str(val).lower() == "true":
            val = True
        else:
            val = False
    else:
        if np.isnan(val):
            val = None
        else:
            if val:
                val = True
            else:
                val = False
    return val


converters = {"Is_LA": as_true_false, "Is_High_LA": as_true_false}
df1 = None
for path in sorted(glob.glob("opioid*gz")):
    print("loading {}....".format(path))
    if df1 is None:
        df1 = pd.read_csv(path, dtype=schema, parse_dates=["month"], date_parser=lambda col: pd.to_datetime(col, utc=True), converters=converters, low_memory=False)
    else:
        df1 = pd.concat([df1, pd.read_csv(path, dtype=schema, parse_dates=["month"], date_parser=lambda col: pd.to_datetime(col, utc=True), converters=converters, low_memory=False)], ignore_index=True)

#for col in ['pct', 'practice', 'status_code', 'chem_substance', 'Is_LA', 'Is_High_LA']:
#    # Because of the concat above, some of these are converted to objects where new categories are introduced
#    # between loop iterations
#    df1[col] = df1[col].astype("category")
for col in ['Is_LA', 'Is_High_LA']:
    # Because of the concat above, some of these are converted to objects where new categories are introduced
    # between loop iterations
    df1[col] = df1[col].astype("category")
# + {}
# import practice list size data

GBQ_PROJECT_ID = '620265099307'

q2 = '''SELECT practice, 
    pct_id AS CCG,
    month,
    total_list_size
from ebmdatalab.hscic.practice_statistics_all_years 
'''

pop = bq.cached_read(q2, csv_path='practice_list_size.zip', use_bqstorage_api=True)
pop['month'] = pd.to_datetime(pop['month'])
pop.head()
# -

df1.head()

# ### Load, organise and filter practice-level data

# +
import pandas as pd
import numpy as np


# convert dates to datetime format
#df1["month"] = pd.to_datetime(df1.month)

# tidy data and replace nulls with appropriate blanks
#df1["High_LA"] = False
#df1.loc[(df1["Is_High_LA"]=="TRUE")|(df1["Is_High_LA"].astype(str)=="True"),"High_LA"] = True
#df1["LA"] = False
#df1.loc[df1["Is_LA"].astype(str)=="True","LA"] = True
  
from pandas.api.types import is_numeric_dtype  
for y in df1.columns:
    if is_numeric_dtype(df1[y].dtype):
          df1[y].fillna(0,inplace=True)


# count included practices and CCGs
print (df1.practice.nunique()," total practices")


# Filter to latest 6 months only and currently active practices with standard CCG codes (for regression and mapping)

dftest = df1.loc[(df1["month"]>df1["month"].max()-pd.DateOffset(months=6))]
print ("Latest ", dftest.month.nunique()," months only (" , df1["month"].max()-pd.DateOffset(months=5), " to ", df1["month"].max(), "):") 
print (dftest.practice.nunique()," practices including those not active / without valid CCG")

dftestc = dftest.loc[dftest["status_code"] == "C"]
print (dftestc.practice.nunique()," practices closed")
dftestd = dftest.loc[dftest["status_code"] == "D"]
print (dftestd.practice.nunique()," practices dormant")

dftest = dftest.loc[(dftest["pct"].str.match(r"([0-9]{2})([A-Za-z])")) & (dftest["status_code"] == "A")]
print (dftest.practice.nunique()," practices included")
print (dftest.pct.nunique()," CCGs")
# -

# ## Practice data for regression analysis

# +
#
# -

# ## Practice trends analysis (monthly data, 2010-2018)

df1.info()

# ### (a) Create calculated fields e.g total OME per 1000 population

# +

df = df1.copy()

# tidy data
df = df.drop(["pct","quantity","net_cost"],axis=1).reset_index(drop=True)

# create columns for high dose and long acting OME
df.loc[df["Is_LA"]==True,"Items Long Acting"] = df["items"]
df.loc[df["Is_High_LA"]==True,"Items High Dose"] = df["items"]
df.loc[df["Is_LA"]==True,"OME Long Acting"] = df["total_ome"]
df.loc[df["Is_High_LA"]==True,"OME High Dose"] = df["total_ome"]
df.loc[df["Is_LA"]==True,"Cost Long Acting"] = df["actual_cost"]
df.loc[df["Is_High_LA"]==True,"Cost High Dose"] = df["actual_cost"]



# +
df2 = df.rename(columns={"total_ome":"Total OME",
                          "items":"Total Items",
                          "actual_cost":"Total Cost"})

df2 = df2.merge(pop[["practice","month","total_list_size"]], on=["practice","month"])
df2["Percent high dose (by OME)"] = 100*df2["OME High Dose"]/df2["OME Long Acting"]
df2["Percent high dose (by items)"] = 100*df2["Items High Dose"]/df2["Items Long Acting"]
df2["Total OME (per 1000)"] = 1000*df2["Total OME"]/df2.total_list_size
df2["Total items (per 1000)"] = 1000*df2["Total Items"]/df2.total_list_size
df2["Total cost (per 1000)"] = 1000*df2["Total Cost"]/df2.total_list_size
df2["Cost high dose opioids (per 1000)"] = 1000*df2["Cost High Dose"]/df2.total_list_size
df2["Cost high dose opioids (per item)"] = df2["Cost High Dose"]/df2["Items High Dose"]

df2["High dose items (per 1000)"] = 1000*df2["Items High Dose"]/df2.total_list_size
df2["Long acting items (per 1000)"] = 1000*df2["Items Long Acting"]/df2.total_list_size
df2["High dose OME (per 1000)"] = 1000*df2["OME High Dose"]/df2.total_list_size
df2["Long acting OME (per 1000)"] = 1000*df2["OME Long Acting"]/df2.total_list_size
df2.head()
# -

# ### (b) Calculate deciles

df3 = df2
x = np.arange(0.1, 1, 0.1) # set range of deciles required (0.1-0.9)
pc = df3.groupby('month').quantile(x)  # calculate deciles for each month
pc = pd.DataFrame(pc.stack()).reset_index().rename(columns={"level_1": 'percentile',"level_2": 'measure', 0:"value"}) # rearrange
pc["index"] = (pc.percentile*10).map(int) # create integer range of percentiles as integers are better for charts
pc.head(12)

# +

import matplotlib.pyplot as plt
import datetime
import matplotlib.gridspec as gridspec
import seaborn as sns

sns.set_style("whitegrid",{'grid.color': '.9'})
dfp = pc.sort_values(by=["month","measure"])
#dfp['month'] = dfp['month'].astype(str)
# set format for dates:
#dfp['dates'] = [datetime.datetime.strptime(date, '%Y-%m-%d').date() for date in dfp['month']]


# set sort order of measures manually, and add grid refs to position each subplot:
s = [(0,'Total OME (per 1000)',0,0,"Oral Morphine Equivalency (mg) per 1000 patients", "(a)  Total OME for all opioid-containing preparations"), 
     (1,'High dose items (per 1000)',0,1,"Items per 1000 patients", "(b)  High dose items"), 
     (2,'Percent high dose (by items)',1,0,"Percent",'(c)  Percent long-acting opioids prescribed as high dose (by items)'), 
     (3,'Percent high dose (by OME)',1,1,"Percent",'(d)  Percent long-acting opioids prescribed as high dose (by OME)'), 
     (4,'Total cost (per 1000)',2,0,"Cost per 1000 patients (GBP)", "(e)  Total cost for all opioid-containing preparations"), 
     (5,'Cost high dose opioids (per 1000)',2,1,"Cost per 1000 patients (GBP)", "(f)  Cost for all high dose opioids")
    ]


fig = plt.figure(figsize=(18,20)) 
gs = gridspec.GridSpec(3,2)  # grid layout for subplots

# Plot each subplot using a loop
for i in s:
    ax = plt.subplot(gs[i[2], i[3]])  # position of subplot in grid using coordinates listed in s
    for decile in range(1,10):   # plot each decile line
        data = dfp.loc[(dfp['measure']==i[1]) & (dfp['index']==decile)]
        if decile == 5:
            ax.plot(data["month"],data['value'],'b-',linewidth=0.7)
        else:
            ax.plot(data["month"],data['value'],'b--',linewidth=0.4)
    ax.set_ylabel(i[4], size =14, alpha=0.6)
    ax.set_title(i[5],size = 17)
    ax.set_ylim([0,1.05*data["value"].max()])
    if  i[4]=="Percent":    # set y axis limit only for percentage measure
        ax.set_ylim([0, 70])
    ax.tick_params(labelsize=12)
    ax.set_xlim([dfp['month'].min(), dfp['month'].max()]) # set x axis range as full date range

plt.subplots_adjust(wspace = 0.13,hspace = 0.16)

#plt.savefig("opioids_Figure4_revised.pdf", transparent=True, dpi=300)  
plt.show()


# -

# ## Variation by CCG (maps)

# ### (a) Aggregate data, create calculated fields

# +
# filter to latest 6 months only
df4 = dftest
df4 = df4.drop(["quantity","net_cost"],axis=1)

# group to ccg level and combine the 6 months
df4 = pd.DataFrame(df4.groupby(["pct","chem_substance","Is_LA","Is_High_LA"])["items","total_ome","actual_cost"].sum()).reset_index()

df4["fent_ome"] = np.where((df4["chem_substance"] == "Fentanyl")& (df4["Is_High_LA"]==True),df4["total_ome"],0)
df4["morph_ome"] = np.where((df4["chem_substance"] == "Morphine Sulfate")& (df4["Is_High_LA"]==True),df4["total_ome"],0)
df4["oxyco_ome"] = np.where((df4["chem_substance"] == "Oxycodone Hydrochloride")& (df4["Is_High_LA"]==True),df4["total_ome"],0)
df4["practice_count"] = 1

# aggregate chem substances
df = pd.DataFrame(df4.groupby(["pct","Is_LA","Is_High_LA"]).sum()).reset_index()

# create columns for high dose and long acting OME
df.loc[df["Is_LA"]==True,"Items Long Acting"] = df["items"]
df.loc[df["Is_High_LA"]==True,"Items High Dose"] = df["items"]
df.loc[df["Is_LA"]==True,"OME Long Acting"] = df["total_ome"]
df.loc[df["Is_High_LA"]==True,"OME High Dose"] = df["total_ome"]
df.loc[df["Is_LA"]==True,"Cost Long Acting"] = df["actual_cost"]
df.loc[df["Is_High_LA"]==True,"Cost High Dose"] = df["actual_cost"]

df = df.drop(["Is_LA","Is_High_LA"],axis=1)
df = df.groupby(["pct"]).sum().reset_index()

df3 = df.rename(columns={"total_ome":"Total OME",
                          "items":"Total Items",
                          "actual_cost":"Total Cost"})

#aggregate list sizes up to CCG level and get population sizes averaged over latest 6 months
popccg = pop.loc[pop["month"]> df1["month"].max()-pd.DateOffset(months=6)]
popccg = popccg.groupby(["CCG","month"])["total_list_size"].sum() # sum across CCGs
popccg = pd.DataFrame(popccg.groupby("CCG").mean()).reset_index() # average across months
df3 = df3.merge(popccg[["CCG","total_list_size"]], right_on="CCG", left_on="pct").drop("CCG",axis=1)

df3["Percent high dose (by items)"] = 100*df3["Items High Dose"]/df3["Items Long Acting"]
df3["Percent high dose (by OME)"] = 100*df3["OME High Dose"]/df3["OME Long Acting"]
df3["Total OME (per 1000)"] = 1000*df3["Total OME"]/df3.total_list_size
df3["Total items (per 1000)"] = 1000*df3["Total Items"]/df3.total_list_size
df3["High dose items (per 1000)"] = 1000*df3["Items High Dose"]/df3.total_list_size
df3["Long acting items (per 1000)"] = 1000*df3["Items Long Acting"]/df3.total_list_size
df3["High dose OME (per 1000)"] = 1000*df3["OME High Dose"]/df3.total_list_size
df3["Long acting OME (per 1000)"] = 1000*df3["OME Long Acting"]/df3.total_list_size
df3["Total cost (per 1000)"] = 1000*df3["Total Cost"]/df3.total_list_size
df3["Cost high dose opioids (per 1000)"] = 1000*df3["Cost High Dose"]/df3.total_list_size
df3.head()

df4 = df3.copy()

df4["% Fentanyl of high dose OME"] = 100*df4["fent_ome"]/df4["OME High Dose"]
df4["% Morphine of high dose OME"] = 100*df4["morph_ome"]/df4["OME High Dose"]
df4["% Oxycodone of high dose OME"] = 100*df4["oxyco_ome"]/df4["OME High Dose"]

df4.head()
# -

# ### (b) Summary table

table = df4[['Total items (per 1000)','Total OME (per 1000)','High dose items (per 1000)',
     'Percent high dose (by items)', 'Percent high dose (by OME)','Total cost (per 1000)',
         '% Fentanyl of high dose OME','% Morphine of high dose OME',
          '% Oxycodone of high dose OME']].agg([min, "median", max]).transpose()
table["fold-difference"] = table["max"]/table["min"]
table

# ### (c) Join to geographical data

# +

spending = df4.copy()
names = pd.read_csv('ccg_for_map.csv')
qc = '''
SELECT 
  code, name
FROM  `hscic.ccgs` c 
WHERE org_type = "CCG"
'''

names = bq.cached_read(qc, csv_path='ccg_names.csv')
spending2 = spending.merge(names[['code','name']],left_on="pct",right_on="code")
spending2 = spending2.drop(["code","fent_ome","morph_ome","oxyco_ome"],axis=1).set_index('name')
spending2 = spending2.round(0)

spending2.sort_values(by="Total OME (per 1000)") # 195 rows
# -

# ### (d) Plot maps

# +
import matplotlib.gridspec as gridspec
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
#from matplotlib import verbose
#verbose.level = 'helpful'    

# from our API https://openprescribing.net/api/1.0/org_location/?org_type=ccg
ccgs = gpd.read_file('ccgs.json').set_index('name')

ccgs = ccgs[~ccgs['geometry'].isnull()]  # remove ones without geometry - these are federations rather than individual CCGs
gdf = ccgs.join(spending2)

# set sort order of measures manually, and add grid refs to position each subplot:
s = [(0,'Total OME (per 1000)',0,0,'(a)  '),      (1,'Total cost (per 1000)',0,1,'(b)  '), 
     (2,'High dose items (per 1000)',1,0,'(c)  '),      (3,'Percent high dose (by items)',1,1,'(d)  ')
    ]

fig = plt.figure(figsize=(16,30))
gs = gridspec.GridSpec(4,2)  # grid layout for subplots

for i in s:
    ax = plt.subplot(gs[i[2], i[3]])  # position of subplot in grid using coordinates listed in s
    gdf.plot(ax=ax,column=i[1],  edgecolor='black', linewidth=0.1, legend=True, cmap='OrRd')
    ax.set_aspect(1.63)
    ax.set_title(i[4]+i[1],size = 20)
    cb_ax = fig.axes[2*(i[0])+1] # extract legend labels from list of axis/legend labels
    cb_ax.tick_params(labelsize=14)
    plt.axis('off')

plt.subplots_adjust(wspace = 0.05,hspace = 0.05)

#plt.savefig("opioids_Figure3ad_revised.pdf", transparent=True, dpi=300)  
plt.show()
# -

# ### (e) Plot additional maps for some percentage measures

# +
import matplotlib.gridspec as gridspec
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd

# set sort order of measures manually, and add grid refs to position each subplot:
s = [(0,'% Fentanyl of high dose OME',0,0,'(e)  ',False),      (1,'% Morphine of high dose OME',0,1,'(f)  ',False), 
     (2,'% Oxycodone of high dose OME',0,2,'(g)  ',True)
    ]

fig = plt.figure(figsize=(17,6))
#plt.subplots(ncols=3)
gs = gridspec.GridSpec(1,3, width_ratios=[4,4,5])  # grid layout for subplots; 
# adjust ratios to allow all plots to appear same size despite the third having a legend.

# set common value limits for colour scale
vmin = 0
vmax = gdf[["% Fentanyl of high dose OME","% Morphine of high dose OME","% Oxycodone of high dose OME"]].max().max()*1.05

for i in s:
    ax = plt.subplot(gs[0,i[0]])  # position of subplot in grid using coordinates listed in s
    gdf.plot(ax=ax,column=i[1],  edgecolor='black', linewidth=0.1, cmap='OrRd', legend = i[5], vmin=vmin, vmax=vmax)   
    ax.set_title(i[4]+i[1],size = 20)
    ax.set_aspect(1.63)  # aspect for correct lat/long
    plt.axis('off')
cb_ax = fig.axes[3] # take legend label (4th item in list of axis/legend labels for all plots)
cb_ax.tick_params(labelsize=16) # set label size for legend

plt.tight_layout()
#plt.savefig("opioids_Figure3eg_revised.pdf", transparent=True, dpi=300)  
plt.show()

# +
import matplotlib.gridspec as gridspec
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
#from matplotlib import verbose
#verbose.level = 'helpful'    

# from our API https://openprescribing.net/api/1.0/org_location/?org_type=ccg
ccgs = gpd.read_file('ccgs.json').set_index('name')

ccgs = ccgs[~ccgs['geometry'].isnull()]  # remove ones without geometry - these are federations rather than individual CCGs
gdf = ccgs.join(spending2)

# set sort order of measures manually, and add grid refs to position each subplot:
s = [(0,'Total OME (per 1000)',0,0,'(a)  '),      (1,'Total cost (per 1000)',0,1,'(b)  '), 
     (2,'High dose items (per 1000)',1,0,'(c)  '),      (3,'Percent high dose (by items)',1,1,'(d)  ')
    ]

fig = plt.figure(figsize=(16,30))
gs1 = gridspec.GridSpec(4,2)  # grid layout for subplots

gs1.update(bottom=0.3, wspace=0.05,hspace = 0.05)

for i in s:
    ax = plt.subplot(gs1[i[2], i[3]])  # position of subplot in grid using coordinates listed in s
    gdf.plot(ax=ax,column=i[1],  edgecolor='black', linewidth=0.1, legend=True, cmap='OrRd')
    ax.set_aspect(1.63)
    ax.set_title(i[4]+i[1],size = 20)
    cb_ax = fig.axes[2*(i[0])+1] # extract legend labels from list of axis/legend labels
    cb_ax.tick_params(labelsize=14)
    plt.axis('off')

#plt.savefig("opioids_Figure3ad_revised.pdf", transparent=True, dpi=300)  
#plt.show()



# set sort order of measures manually, and add grid refs to position each subplot:
s = [(0,'% Fentanyl of high dose OME',0,0,'(e)  ',False),      (1,'% Morphine of high dose OME',0,1,'(f)  ',False), 
     (2,'% Oxycodone of high dose OME',0,2,'(g)  ',True)
    ]

#fig = plt.figure(figsize=(17,6))
#plt.subplots(ncols=3)
gs2 = gridspec.GridSpec(1,3, width_ratios=[4,4,5])  # grid layout for subplots; 
# adjust ratios to allow all plots to appear same size despite the third having a legend.
gs2.update(top=0.3,wspace=0.05)

# set common value limits for colour scale
vmin = 0
vmax = gdf[["% Fentanyl of high dose OME","% Morphine of high dose OME","% Oxycodone of high dose OME"]].max().max()*1.05

for i in s:
    ax = plt.subplot(gs2[0,i[0]])  # position of subplot in grid using coordinates listed in s
    gdf.plot(ax=ax,column=i[1],  edgecolor='black', linewidth=0.1, cmap='OrRd', legend = i[5], vmin=vmin, vmax=vmax)   
    ax.set_title(i[4]+i[1],size = 18)
    ax.set_aspect(1.63)  # aspect for correct lat/long
    plt.axis('off')
cb_ax = fig.axes[3] # take legend label (4th item in list of axis/legend labels for all plots)
cb_ax.tick_params(labelsize=16) # set label size for legend

plt.tight_layout()
#plt.savefig("opioids_Figure3eg_revised.png", format='png', dpi=300)
plt.show()
# -

# ## Potential cost savings

# +
# Extract lowest practice percentile for both high-dose items and cost per 1000 population for latest 6 months
lowest_dec = pc[["month","measure","value"]].loc[(pc["month"]>pc["month"].max() - pd.DateOffset(months=6)) 
       & (pc["percentile"] == 0.1)].fillna(0)
lowest_dec.loc[lowest_dec["measure"]=="High dose items (per 1000)","measure"] = "high dose items"
lowest_dec.loc[lowest_dec["measure"]=="Cost high dose opioids (per 1000)","measure"] = "high dose cost"
lowest_dec.loc[lowest_dec["measure"]=="Cost high dose opioids (per item)","measure"] = "high dose costperitem"

# extract data on high dose items
dfs = pd.DataFrame(df2[["practice","month","total_list_size","Items High Dose","Total Cost","Cost High Dose", "Cost high dose opioids (per item)"]].set_index(["practice","month","total_list_size"]).stack()).reset_index().rename(columns={"level_3":"measure",0:"actual_value"})
dfs.loc[dfs["measure"]=="Items High Dose","measure"] = "high dose items"
dfs.loc[dfs["measure"]=="Cost High Dose","measure"] = "high dose cost"
dfs.loc[dfs["measure"]=="Cost high dose opioids (per item)","measure"] = "high dose costperitem"

# merge with lowest decile data (inner join, this restricts data to latest 6 months)
savings = dfs.merge(lowest_dec,on=["month","measure"])

savings["target_value"] = savings["total_list_size"]*savings["value"]/1000
savings["potential_saving"] = savings["actual_value"] - savings["target_value"]
savings = savings.fillna(0)
savings.loc[savings["potential_saving"]<0,"potential_saving"] = 0

print("Practices at the lowest decile over the latest 6 months prescribed: ", lowest_dec.loc[lowest_dec["measure"]=="high dose items"].mean().round(2).value, 
      "items / £", lowest_dec.loc[lowest_dec["measure"]=="high dose cost"].mean().round(2).value, " per 1,000 patients")

print(savings.groupby("measure")["actual_value","potential_saving"].sum())

# +
# Compare best cost-per-item prices
dfs2 = pd.DataFrame(df2[["practice","month","Items High Dose","Total Cost","Cost High Dose", "Cost high dose opioids (per item)"]])
#dfs2.loc[dfs2["measure"]=="Cost high dose opioids (per item)","measure"] = "high dose costperitem"
savings = dfs2.merge(lowest_dec.loc[lowest_dec.measure=="high dose costperitem"],on=["month"])
savings["target_value"] = savings["Items High Dose"]*savings["value"]
savings["potential_saving"] = savings["Cost High Dose"] - savings["target_value"]

savings = savings.fillna(0).drop("measure",axis=1)
savings.loc[savings["potential_saving"]<0,"potential_saving"] = 0



print("If each practice prescribed at the lowest decile cost-per-item over the latest six months (£", 
      lowest_dec.loc[lowest_dec["measure"]=="high dose costperitem"].mean().round(2).value, 
      "per item), in total they could have saved £", round(savings["potential_saving"].sum(),-3), " out of a total £",
      round(savings["Cost High Dose"].sum(),-3), "(",
      round(100*savings["potential_saving"].sum()/savings["Cost High Dose"].sum(),1), "%)" )

# -

# ## Change by CCG

# +
yr1 = df1.loc[(df1["pct"].str.match(r"([0-9]{2})([A-Za-z])"))& (df1['month'].dt.year>=2016)].reset_index()
yr1["year"] = yr1['month'].dt.year

yr1.head()
# -

df1.info()

yr1 = df1.loc[(df1["pct"].str.match(r"([0-9]{2})([A-Za-z])"))& (df1['month'].dt.year>=2016)].reset_index()
yr1["year"] = yr1['month'].dt.year
yr1.head()

# + {"active": ""}
#
# -

yr2 = yr1[["pct","practice","month","year","total_ome"]].groupby(["pct","practice","month","year"])['total_ome'].sum()
yr2 = yr2.reset_index().merge(pop[["practice","month","total_list_size"]], on=["practice","month"]).groupby(["pct","month","year"]).sum()
yr2.head()

yr2 = yr2.reset_index().groupby(['pct','year']).agg({'total_list_size':'mean','total_ome':"sum"})
yr2["OME_per_1000"] = 1000*yr2["total_ome"]/yr2.total_list_size
yr2 = yr2.drop(['total_list_size','total_ome'],axis=1).unstack().reset_index(col_level=1)
yr2.columns = yr2.columns.droplevel()
yr2 = yr2.drop(2018,axis=1)
yr2["change"] = yr2[2017]-yr2[2016]
yr2["%change"] = 100*(yr2["change"])/yr2[2016]
yr2 = yr2.sort_values(by="change").reset_index(drop=True)
yr2.head()

# +
change = yr2.copy()

change2 = change.merge(names[['code','name']],left_on="pct",right_on="code")
change2 = change2.drop([2016,2017,"code"],axis=1).set_index('name')

change2.sort_values(by="change") # 195 rows

# +
import matplotlib.gridspec as gridspec
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
#from matplotlib import verbose
#verbose.level = 'helpful'    
import matplotlib.colors as colors

cmap = 'coolwarm'
#bounds = np.array([-60000,-45000, -30000, -15000, 0, 0, 5000, 10000,15000, 20000])
#norm = colors.BoundaryNorm(boundaries=bounds, ncolors=256)


# from our API https://openprescribing.net/api/1.0/org_location/?org_type=ccg
ccgs = gpd.read_file('ccgs.json').set_index('name')

ccgs = ccgs[~ccgs['geometry'].isnull()]  # remove ones without geometry - these are federations rather than individual CCGs
gdf = ccgs.join(change2)

vmax = -gdf["change"].min() # set max of scale to reflect minimum such that zero falls in centre of scale.

fig, ax = plt.subplots(figsize=(12, 7))
g = gdf.plot(ax=ax,column="change",  edgecolor='black', linewidth=0.1, legend=True, cmap=cmap, vmax=vmax)
ax.set_aspect(1.63)
#cb_ax = fig.axes[2*(i[0])+1] # extract legend labels from list of axis/legend labels
#cb_ax.tick_params(labelsize=14)
plt.axis('off')
#fig.colorbar(g, ax=ax, extend='both', orientation='vertical')

plt.show()

# -

# # All practice types

# +
q5 = '''
-- opioids - including all practices

WITH q2 AS (
  SELECT 
    s.practice, 
    s.pct_id AS pct,
    CAST(s.month AS DATE) AS year_mon, 
    CASE WHEN prac.setting = 4 THEN "GP" ELSE "other" END AS type,
    MAX(total_list_size) AS total_list_size
  FROM ebmdatalab.hscic.practice_statistics_all_years s
  INNER JOIN  ebmdatalab.hscic.practices prac ON prac.code=s.practice 

  GROUP BY practice, pct, type, year_mon )

-- join practices to opioid prescribing data and group to CCGs
SELECT 
  COALESCE(p.pct,q2.pct) AS pct,
  q2.year_mon AS month,
  type,
  l.chem_substance,
  Is_LA,
  Is_High_LA,
  sum(p.items) as items, 
  sum(quantity) as quantity,
  sum(quantity*dose_per_unit*new_ome_multiplier) AS total_ome,
  sum(net_cost) as net_cost,
  sum(actual_cost) as actual_cost
FROM q2 
LEFT JOIN ebmdatalab.helen.opioid_prescribing_2010_2018 p ON p.practice = q2.practice AND CAST(p.month AS DATE) = q2.year_mon  
INNER JOIN (SELECT distinct bnf_code, chem_substance, Is_High_LA, Is_LA, dose_per_unit, new_ome_multiplier FROM ebmdatalab.richard.opioid_converter) l on l.bnf_code = p.bnf_code
INNER JOIN `hscic.ccgs` c ON c.code = p.pct AND org_type = "CCG"

GROUP BY 
  pct,
  type,
  month,
  chem_substance,
  Is_LA,
  Is_High_LA
'''

'''opioid prescribing data extracted as follows:
select *
FROM ebmdatalab.hscic.normalised_prescribing_standard p
WHERE SUBSTR(p.bnf_code,1,6) IN ("040201","040702","100101")'''


ccg = bq.cached_read(q5, 'opiods_by_practice_setting.csv.gz', use_bqstorage_api=True)
ccg.head()
# -

c2 = ccg.copy()#.loc[ccg["Is_High_LA"]=="TRUE"]
c2["month"] = pd.to_datetime(c2.month)
c2 = c2.loc[c2["month"]>c2["month"].max() - pd.DateOffset(months=6)]
c2 = pd.DataFrame(c2.groupby(["pct","type"])["total_ome"].sum()).unstack() #.sort_values(by="total_ome").transpose()    # "pct","Is_LA","Is_High_LA"c
c2.columns = c2.columns.droplevel()
c2["ome_percent_nongp"] = 100*c2.other / (c2.other+c2.GP)
print(c2["ome_percent_nongp"].count(), 'CCGs have data on opioid prescribing from non-standard practices,')
print('accounting for a maximum of ',c2["ome_percent_nongp"].max().round(3), '% of CCG opioid prescribing')

# ### Which opioids are high-dose

# +
q = '''

SELECT 
  chem_substance,
  bnf_code,
  drug_name,
  dose_per_unit as total_dose_per_unit_mg,
  LA_doseshourduration as duration_hrs,
  LA_daily_dose as daily_dose,
   new_ome_multiplier as ome_multiplier,
  LA_daily_OME as daily_OME

FROM ebmdatalab.richard.opioid_converter
WHERE Is_High_LA IN ("True","TRUE")

 
order by chem_substance, dose_per_unit desc
  
    '''


tbl = bq.cached_read(q, csv_path='opioids_high_dose_formulations.csv')
tbl.head()
# -


