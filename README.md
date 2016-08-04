Severity, Cost and Norm cost Binnacle
===============================

----------


# **Index**
-------------
1. [Abstract_1 - Creation of flat tables bodyshops_pay and full_pay](#**Abstract_1)
2. [Data_sources](#**Data_sources**)
3. [Flat tables creation](#Flat)
4. [Abstract_2 - Severity and norm cost](#Severity)
5. [Main.py](#Main.py)
6. [bodyshop.py](#bodyshop.py)
7. [severity.py](#severity.py)
8. [norm_cost.py](#norm_cost.py)

----------


#**Abstract_1 - Creation of flat tables bodyshops_pay and full_pay**
-------------
First of all, there is a full explanation, based on functional requeriments, data sources and flowcharts for building flat tables **bodyshops_pay** and **full_pay**. During this record, there is the chance of making a reproductible example for recreating datasources, kpis, severity and norm cost in the same way Axa Spain does.

----------


#**Data_sources**
-------------
In order to create both flat tables, there are following  tables involved in the process.
Next it will be found data sources, roots from Hue, formats, and number of rows and columns in order to ensure model is reproductible.

Data Source 1: **Full expert 2014 - 2015**
Name: **enc_per_perim_2014_2015.csv**
Root: **/group/claims/CCA_phase2/data_raw/full_expert_order**
Size: **1.4 GB**
Shape (number of columns and rows):  **188 * 1350733**
Column names:
![colnames(FULL1415).PNG](#file:6985829f-9602-b158-0260-d1b98b63319a)


Data Source 2: **Payments 2014 - 2015**
Name: **pagos_1415_ok.csv**
Root: **/group/claims/CCA_phase2/data_raw/payments**
Size: **272 Mb**
Shape (number of columns and rows): **944475 * 83**
Column names:
![colnames(pagos).PNG](#file:3a62525a-8a79-bc29-793b-7e87bb7c442f)


Data Source 3: **Claims 2014 - 2015**
Name: **claims_axa_20151231.csv**
Root: **/group/claims/CCA_phase2/data_raw/claims/axa**
Size: 
Shape (number of columns and rows): **39 * x** 
Column names:
![colnames(claims).PNG](#file:ec69daf6-a92d-648f-9062-9f963f785297)

Data Source 4: **Claims 2013 - 2014**
Name: **claims_axa_20141231.csv**
Root: **/group/claims/CCA_phase2/data_raw/claims/axa**
Size: **662 Mb **
Shape (number of columns and rows): **39 * x** 
Column names:
![colnames(claims).PNG](#file:ec69daf6-a92d-648f-9062-9f963f785297)

Data Source 5: **Providers Geolocation**
Name: **providers_completed.csv**
Root: **/group/claims/CCA_phase2/data_raw/providers/peri_prove_web** NOT HERE
Size: **594 Mb **
Shape (number of columns and rows): 
Column names:
![colnames(provider_geo).PNG](#file:5db686b2-f9a5-81f0-80e0-ae820c15e8b2)

Data Source 6: **Providers information**
Name: **peri_prove_web_201512.csv**
Root: **/group/claims/CCA_phase2/data_raw/providers/peri_prove_web**
Size:  **28.4 Mb**
Shape (number of columns and rows):  **58 * 109801**
Column names:
![colnames(providers).PNG](#file:62a2c123-27cc-25d2-d4e0-e5e2a0860840)

Data Source 7: **Mosaic**
Name: **mosaic_info_hdfs.csv**
Root: **/group/claims/CCA_phase2/data_raw/mosaic**
Size: **539.7 Kb**
Shape (number of columns and rows): **10 * 10962**
Column names:
![colnames(mosaic).PNG](#file:bdf0b770-9b8d-4175-5c7e-b9d7d2c627e8)

Data Source 8: **Policies_Axa ** 
Name: **iard_1512_clean_ok.csv**
Root: **/group/claims/CCA_phase2/data_raw/policies/axa**
Size: **182.6 Mb**
Shape (number of columns and rows):**19 * 2200862 **
Column names:
![colnames(policies_axa).PNG](#file:36dbf6d8-535c-f568-a42d-97bf1760f5da)

Data Source 9: **Policies_Direct** 
Name: **iard_1512_clean_ok.csv**
Root: **/group/claims/CCA_phase2/data_raw/policies/direct**
Size: **111.0 Mb**
Shape (number of columns and rows):**30 * 587153**
Column names:
![colnames(policies_direct).PNG](#file:c7d4cc4f-c82a-6fa0-5156-e90cafb2a12f)

Data Source 9: **Variables related to vehicles** 
Name: **vehiculos_1512.csv**
Root: **/group/claims/CCA_phase2/data_raw/vehicles**
Size: **20.9 Mb**
Shape (number of columns and rows):**66* 85787**
Column names:
![colnames(vehiculos).PNG](#file:e92b12ab-e144-bf52-7192-f43e42d9be84)

----------


# Flat tables creation
-------------
First step is reading from HUE the following table:

1. enc_per_perim_2014_2015.csv

After reading the file, business filters are applied. Those are defined in the following extract of code:

```
#### FULL EXPERT ORDER
library(data.table)
path <- "..."  #local path where csv is stored into Rstudio Environment

## 2014 - 2015
name <- paste0(path, "enc_per_perim_2014_2015.csv")
full <- fread(name, sep=";", colClasses = c(rep("character", 188)))

## FILTER
full <- full[EstEncar %in% c("M", "R"), ] 
full <- full[Rsp_PT == "0", ]
full <- full[Rsp_Des_Rep == "0", ] 
full <- full[Rsp_Fal == "0", ]
full <- full[!TipEncar %in% c("3", "4", "5","7"), ]
full <- full[Rsp_Imp_Tra != "0", ]
full <- full[Sin_Ram == "2", ]
full <- full[Sin_Emp == "0", ]
full <- full[Sin_Poz == "0", ]
full <- full[Rsp_Fra_Tip %in% c("0", "3"), ]
full <- full[Sin_Eve == "0", ]
full <- full[!Sin_Eti_Agr %in% c("PERDIDA TOTAL", "PERDIDA TOTAL RC", "PJ"), ]
full <- full[Veh_Cat == "1", ]
full <- full[Tll_Prf == "11", ]
full <- full[!CdTaller %in% c("", "0", "46", NA), ]
full <- full[Tll_Cod_Pro != "62", ] #ANDORRA
full[, ImpTotPr := as.numeric(ImpTotPr)]
full[, ImpMoCh := as.numeric(ImpMoCh)]
full[, ImpMater := as.numeric(ImpMater)]
full[, ImpMoPt := as.numeric(ImpMoPt)]
full[, ImpMatPt := as.numeric(ImpMatPt)]
full <- full[ImpTotPr <= 3000,]
full <- full[(ImpMoCh + ImpMater + ImpMoPt + ImpMatPt > 0),]
```

After reading table and ensuring it pass properly each filter, size of data.table should be:

1.  nrow(full) = 800838

Consequently, next step is defined by adding new variables computed afterwards.
Following piece of code define this functional requeriment:
```
#"Mat_Num_DI" "Mat_Imp_DI" "Mat_Aho_DI" "Sin_CdP"    "Sin_Rec" 
full[, Mat_Num_DI := NULL]
full[, Mat_Imp_DI := NULL]
full[, Mat_Aho_DI := NULL]
full[, Sin_CdP := NULL]
full[, Sin_Rec := NULL]
```

Last pre-formating step is defining setkeys and removing possible duplicates. Those keys are Idsin, NmEnca and ImpTotPr.

```
setkey(full, Idsin, NmEnca, ImpTotPr)
full <- unique(full)
rm(full)
```
Workflow is after this point the following:

## INSERT DATA FLOW IMAGE HERE


It starts joining data tables in order to enrich the data source.
First one is payments, as the following:

```
path <- "..."  #local path where csv is stored into Rstudio Environment
name <- paste0(path, "pagos_1415_ok.csv")
pagos <- read.csv2(name, header = T, sep=";")
pagos <- as.data.table(pagos)
```

By defining just variables to be joined with **full**, same step is formatting both keys in the same types, creating the setkeys in both tables and merging them into a left outer join 


```
## full y payments
keep = c("Pag_Sin", "Pag_Imp", "Enc_Num")
pagos <- pagos[, keep, with=F]
setnames(pagos, colnames(pagos), c("Idsin", "ImpTotPr", "NmEnca")) 

full[,Idsin := as.character(Idsin)]
full[, ImpTotPr := as.numeric(ImpTotPr)]
full[, NmEnca := as.character(NmEnca)]

pagos[,ImpTotPr := as.numeric(as.character(ImpTotPr))]
pagos[,NmEnca := as.character(NmEnca)]
pagos[,Idsin := as.character(Idsin)]


setkey(pagos, Idsin, NmEnca, ImpTotPr)
pagos <- unique(pagos)
setkey(full, Idsin, NmEnca, ImpTotPr)
full <- unique(full)

full <- full[pagos]

table(full$Idsin %in% pagos$Idsin) 
table(pagos$Idsin %in% full$Idsin)

# We just want rows when is.na(ImpNetrp) == F 
full <- full[which(!is.na(ImpNetrp)), ]
```
After this point, nrow(full) should be 652322
Workflow is after this point the following:

## INSERT DATA FLOW IMAGE HERE

Once full dataset is merged with payments,  claims datasets are loaded. There are data from 2014 non appearing in  claims_axa_20151231.csv, hence there is the need of loading claims_axa_20141231.csv in order to have the whole information.
Once each file is readed, only few variables are kept and renamed in order to mantain  structure before binding both tables.

```
# Claims
path <- "..." #local path where csv is stored into Rstudio Environment
name <- paste0(path, "claims_axa_20151231.csv")
claims <- read.csv2(name, header = T, sep="|")
claims <- as.data.table(claims)

keep = c("Cia", "Expdte", "Situacion", "LesionMaterial", "focurr", "FAlta", 
         "FTERM", "Agente", "OFI", "REG", "ProductoAs", "CategVeh", "Proces", 
         "Naturaleza", "TIPO", "POLIZA", "CanalApertura")
claims <- claims[, keep, with = F]

colnames(claims) <- c("Cia", "Expdte", "Situacion", "LesionMaterial", "FOcurr", "FAlta", 
         "FTerm", "Agente", "Ofi", "Reg", "ProductoAs", "CategVeh", "Proces", 
         "Naturaleza", "Tipo", "Poliza", "CanalApertura")

name <- paste0(path, "claims_axa_20141231.csv")
claims_old <- read.csv2(name, header = T, sep="|")
claims_old <- as.data.table(claims_old)
0
keep = c("Cia", "Expdte", "Situacion", "LesionMaterial", "focurr", "FAlta", 
         "FTERM", "Agente", "OFI", "REG", "ProductoAs", "CategVeh", "Proces", 
         "Naturaleza", "TIPO", "POLIZA", "CanalApertura")
claims_old <- claims_old[, keep, with = F]

colnames(claims_old) <- c("Cia", "Expdte", "Situacion", "LesionMaterial", "FOcurr", "FAlta", "FTerm", "Agente", "Ofi", "Reg", "ProductoAs", "CategVeh", "Proces", "Naturaleza", "Tipo", "Poliza", "CanalApertura")


claims <- rbind(claims, claims_old)
claims <- unique(claims, by = "Expdte")
```
nrow(claims) should be 4370121

Column names are redefined in order to be aligned with the dataset and afterwards it is left outer joined with full by Sinister Id.  It is checked weird orders, i.e. orders when "claims_Cia" is null. nrow(Weird_orders) must be zero.  
```

setnames(claims, colnames(claims), paste0("claims_", colnames(claims)))
setnames(claims, "claims_Expdte", "Idsin")
claims[,Idsin := as.character(Idsin)]
setkey(full, Idsin)
setkey(claims, Idsin)
full1 <- claims[full]
weird_orders <- full1[which(is.na(claims_Cia)), ]
full1 <- full1[which(!is.na(claims_Cia)), ]

# Check number of claims in full and vice versa to know how well are they going to merge
table(claims$Idsin %in% full$Idsin)
table(full$Idsin %in% claims$Idsin)

```
Once both joins are made, nrow(full1) might be 652322
Workflow is after this point the following:

## INSERT DATA FLOW IMAGE HERE

At this point, provider geolocation is joined with provider dataset, column names are defined to be aligned with full and both are joined by CdTaller (bodyshop id).
Rows when geolocation are not available are deprecated. 
```
# full & providers, both in local path, where csv is stored into Rstudio Environment
provider_geo <- fread("~/providers_completed.csv")  #local path where csv is stored into Rstudio Environment
provider <- fread("~/peri_prove_web_201512.csv")    #local path where csv is stored into Rstudio Environment


keep <- c("Id_Proveedor", "Latitudes", "Longitudes")
provider_geo <- provider_geo[, keep, with = F]

provider[, Id_Proveedor := as.character(Id_Proveedor)]
provider_geo[, Id_Proveedor := as.character(Id_Proveedor)]

setkey(provider_geo, Id_Proveedor)
setkey(provider, Id_Proveedor)
provider <- provider_geo[provider]
setnames(provider, colnames(provider), paste0("provider_", colnames(provider)))
setnames(provider, "provider_Id_Proveedor", "CdTaller")
setkey(full1, CdTaller)
setkey(provider, CdTaller)
full1 <- provider[full1]
full1 <- full1[which(!is.na(provider_Longitudes)), ]
```
Once both joins are made, nrow(full1) might be 652089
Workflow is after this point the following:

## INSERT DATA FLOW IMAGE HERE

Mosaic data is added to full by checking shared zip code.  Then Enc_Sol_Nom is added to full.
```
# full y mosaic
mosaic <- fread("~/mosaic.csv") #local path where csv is stored into Rstudio Environment
keep <- c("codigo_postal", "poblacion", poblacion_masculina, "mosaic_numero", "georisk_grupo", "georisk_media", "area")
mosaic <- mosaic[keep, with = FALSE]
names(mosaic) <- c("Tll_Cod_Pos","mosaic_population", "mosaic_code_det",   "mosaic_georisk_gr", "mosaic_georisk_av", "mosaic_area")

full1[, Tll_Cod_Pos := as.numeric(Tll_Cod_Pos)]
mosaic[, mosaic_postcode := as.numeric(mosaic_postcode)]
setnames(mosaic, "mosaic_postcode", "Tll_Cod_Pos")
setkey(full1,Tll_Cod_Pos)
setkey(mosaic, Tll_Cod_Pos)
full1 <- mosaic[full1]
full1[, Enc_Sol_Nom:=NULL]
```
After this point, nrow(full1) might be 652089
Workflow is after this point the following:

## INSERT DATA FLOW IMAGE HERE


Here comes now joining with policies from Axa and Direct. Once column names from Axa and Direct are defined and binded into a new table, it is time to be joined with fullin order to separate policies from each business. 

```
polizas <- read.csv2("~/iard_1512_clean_ok.csv", header = T)
polizas <- as.data.table(polizas)

direct <-  fread("~/Direct_2015_ok.csv")

pol_idenpn <- polizas[, c("IDENPN", "POLIZA"), with = F]
pol_idenpn <- unique(pol_idenpn)

names(direct)[1] <- "IDENPN"
pol_idenpn_direct <- direct[, c("IDENPN", "POLIZA"), with = F]
pol_idenpn_direct <- unique(pol_idenpn_direct)

pol_idenpn <- rbind(pol_idenpn, pol_idenpn_direct)

## we assume the following:
pol_idenpn <- unique(pol_idenpn, by = "POLIZA")

setnames(pol_idenpn, "POLIZA", "poliza")
setnames(full1, "claims_Poliza", "poliza")
setkey(pol_idenpn, poliza)
setkey(full1, poliza)
full1 <- pol_idenpn[full1]
```
After this join, nrow(full1) might be 652089.
All data related  to cars, such as brand, model, version... are taken from policies aswell, so the extraction of this data can be checked in the following lines of code.
```
### Vehicles information
## Esta en "polizas" y en "direct"
polizas[, RAMO := NULL]
polizas[, COMPANIA := "As"]
polizas[, MARCA := gsub("^[0]+", "", MARCA) ]
polizas[, MDL := gsub("^[0]+", "", MDL) ]
polizas[, VERSION := gsub("^[0]+", "", VERSION) ]
polizas <- unique(polizas, by = c("POLIZA", "MARCA", "MDL", "VERSION"))
setnames(polizas, colnames(polizas), paste0("polaxa_", colnames(polizas)))


direct[, COMPANIA := "Ds"]
direct[, MARCA_ID := gsub("^[0]+", "", MARCA_ID) ]
direct[, MODELO_ID := gsub("^[0]+", "", MODELO_ID) ]
direct[, VERSION_ID := gsub("^[0]+", "", VERSION_ID) ]
direct[, direct_year := 2015]
setnames(direct, "VERSION", "VERSION_TEXT")
direct <- unique(direct, by = c("POLIZA", "POLIZA_VERSION", "MARCA_ID", "MODELO_ID", "VERSION_ID"))
setnames(direct, c("POLIZA_VERSION", "MARCA_ID", "MODELO_ID", "VERSION_ID", "direct_year"), c("PRODUCTO", "MARCA", "MDL", "VERSION", "polizas_year"))
setnames(direct, colnames(direct), paste0("poldirect_", colnames(direct)))
direct <- direct[order(-poldirect_polizas_year)]
direct <- unique(direct, by = "poldirect_POLIZA")
```
Now both tables (policies and full) are joined, assuming both vehicle policie and product are unique.
```
setkey(full1, claims_ProductoAs, poliza)
setkey(polizas, polaxa_PRODUCTO, polaxa_POLIZA)
full1 <- polizas[full1]

setnames(full1, c("polaxa_POLIZA", "polaxa_PRODUCTO"), c("poliza", "producto"))

#setnames(direct, "poldirect_POLIZA", "poliza")

# we assume the following:
#pol_veh <- unique(pol_veh, by=c("poliza"))
#pol_veh_direct <- unique(pol_veh_direct, by = c("poliza"))

#setkey(pol_veh, poliza)
#setkey(full, poliza)
#full <- pol_veh[full]


#setkey(pol_veh_direct, poliza)
#setkey(full, poliza)
#full <- pol_veh_direct[full]
setkey(full1, poliza, Sin_Cia)
setkey(direct, poldirect_POLIZA, poldirect_COMPANIA)
full1 <- direct[full1]
setnames(full1, c("poldirect_POLIZA", "poldirect_PRODUCTO", "poldirect_COMPANIA"), c("poliza", "producto", "Sin_Cia"))

```
Variables related to Axa are coalesce to null when policie is from Direct and viceversa, and new variables are defined to be filled afterwards.

```
full1[, ve_marca_id := ifelse(is.na(polaxa_MARCA), poldirect_MARCA, polaxa_MARCA)]
full1[, ve_modelo_id := ifelse(is.na(polaxa_MDL), poldirect_MDL, polaxa_MDL)]
full1[, ve_version_id := ifelse(is.na(polaxa_VERSION), poldirect_VERSION, polaxa_VERSION)]
#full[, ve_marca := ifelse(is.na(polaxa_LMARCA), poldirect_MARCA_DESC, polaxa_LMARCA)]
#full[, ve_modelo := ifelse(is.na(polaxa_LMODELO), poldirect_MODELO_DESC, polaxa_LMODELO)]
#full[, ve_version := ifelse(is.na(polaxa_LVERSION), poldirect_VERSION, polaxa_LVERSION)]
full <- full1
full[, polaxa_MARCA := NULL]
full[, polaxa_MDL := NULL]
full[, polaxa_VERSION := NULL]
full[, poldirect_MARCA := NULL]
full[, poldirect_MDL := NULL]
full[, poldirect_VERSION := NULL]
#full[, polaxa_LMARCA := NULL]
#full[, polaxa_LMODELO := NULL]
#full[, polaxa_LVERSION := NULL]
#full[, poldirect_MARCA_DESC := NULL]
#full[, poldirect_MODELO_DESC := NULL]
#full[, poldirect_VERSION := NULL]

full <- full[which(!is.na(ve_marca_id)), ]
```
After this point, nrow(full) might be 366124
Workflow is after this point the following:

## INSERT DATA FLOW IMAGE HERE

There are few variables related to vehicles out of policies must be included. Those are merged by vehicle brand,  vehicle model and vehicle version. This is the last merge used during the creation of flat tables in the process-
Rows when vehiculos_PVENTA is null drops off the table.
```
# full y vehiculos
vehiculos <-  fread("~/vehiculos_1512.csv")
setnames(vehiculos, colnames(vehiculos), paste0("vehiculos_", colnames(vehiculos)))
setnames(vehiculos, c("vehiculos_MARCA", "vehiculos_MODELO", "vehiculos_VERSION"), c("ve_marca_id", "ve_modelo_id", "ve_version_id"))
vehiculos[, ve_marca_id := gsub("^[0]+", "", ve_marca_id) ]
vehiculos[, ve_modelo_id := gsub("^[0]+", "", ve_modelo_id) ]
vehiculos[, ve_version_id := gsub("^[0]+", "", ve_version_id) ]

full[, ve_marca_id := gsub("^[0]+", "", ve_marca_id) ]
full[, ve_modelo_id := gsub("^[0]+", "", ve_modelo_id) ]
full[, ve_version_id := gsub("^[0]+", "", ve_version_id) ]

setkey(vehiculos, ve_marca_id, ve_modelo_id, ve_version_id)
setkey(full, ve_marca_id, ve_modelo_id, ve_version_id)

full <- vehiculos[full]
full <- full[which(!is.na(vehiculos_PVENTA)), ]
```
Once all joins are made, nrow(full) might be 366018
Workflow is after this point the following:

## INSERT DATA FLOW IMAGE HERE

Now comes feature engineering. First is like in the following extract of code, set names for variables.

```
##IDs
setnames(full, "Idsin", "claim_id")
setnames(full, "NmValora", "assessment_id")
setnames(full, "NmEnca", "expert_order_id")
setnames(full, "Val_Num_Hos", "host_number_id")
setnames(full, "poliza", "policy_id")
setnames(full, "Enc_Num_Web", "order_web_id")
## setnames(full, "Enc_Num_Tir",    #######
## setnames(full, "Enc_Num_Ori",    #######
setnames(full, "CdTaller", "garage_id")

#CLIENT
setnames(full, "Sin_CVM", "client_value_claim")
setnames(full, "IDENPN", "client_id") ## quizas no esta

## GOAL
setnames(full, "Rep_Obj_DFn","goal_end_depot")               
setnames(full, "Rep_Obj_IFn","goal_end_start")   
setnames(full, "Obj_RS_Con","goal_pnd_contact")         
setnames(full, "Obj_RS_Prv","goal_pnd_appointment") 
setnames(full, "Enc_Obj_PRs","goal_first_resp")              
setnames(full, "Enc_Obj_URs","goal_last_resp") 
  
### CLAIM
setnames(full, "Sin_Nat", "claim_type")
setnames(full, "Sin_Nat_0", "claim_type_code")
setnames(full, "Fra_Tip", "claim_fraud_type")
setnames(full, "Fra_Tip_Pos", "claim_fraud")
setnames(full, "Rsp_Per", "claim_year_month")
setnames(full, "Sin_Cul", "claim_guilty")
setnames(full, "Sin_Tip_Ape", "claim_opening_type")
setnames(full, "Sin_Can_Ape", "claim_opening_channel_code")
setnames(full, "Sin_Eti_Agr", "claim_label")
setnames(full, "Sin_Cia", "claim_company")
full[, Sin_Emp := NULL]
full[, Sin_Poz := NULL]
full[,Sin_Eve := NULL]
setnames(full, "claims_FTerm", "claim_date_ending")                                
full[,claims_Cia := NULL]
setnames(full, "claims_Situacion", "claim_status")
setnames(full, "claims_Proces", "claim_material")
full[, claims_Tipo := NULL]
full[, claims_LesionMaterial := NULL]
setnames(full, "claims_FOcurr", "claim_date_ocur")
setnames(full, "claims_FAlta", "claim_date_opening")
setnames(full, "claims_CanalApertura", "claim_opening_channel")
full[,claims_Agente := NULL ] 
full[, claims_Ofi := NULL]    
full[, claims_Reg := NULL]
full[, claims_ProductoAs := NULL]
full[, claims_CategVeh := NULL]  
setnames(full, "claims_Naturaleza", "claim_nature")   
#full[, claims_MATRI := NULL]
#full[, claims_claims_year := NULL]  
setnames(full, "Rep_Veh_Sus", "claim_replacement_vehicle")
setnames(full, "Rep_Veh_Rec", "claim_pnd_service")
setnames(full, "Mat_Aho_DI", "claim_saving_di")  ## puede q no este
setnames(full, "Sin_Num_Ori", "claim_main_id")                                           
setnames(full, "Sin_CdP", "claim_zipcode")  ## puede q no este
setnames(full, "Fra_Imp_Aho", "claim_saving_fraud")
setnames(full, "Act_RS_Id", "claim_pnd_activiy")

### VEHICLE
full[, ve_brand := vehiculos_LMARCA]
full[, vehiculos_LMARCA := NULL]
full[, Veh_Mar := NULL]

full[, ve_model := vehiculos_LMODELO]
full[, vehiculos_LMODELO := NULL]

full[, ve_version := vehiculos_LVERSION]
full[, vehiculos_LVERSION := NULL]
full[, ve_version := gsub(";", "", ve_version)]

full[, vehiculos_CATVEH := NULL]

setnames(full, "Veh_Cla", "ve_class_id")
full[, vehiculos_CLASE := NULL]

setnames(full, "vehiculos_NPUERTAS", "ve_ndoors")
setnames(full, "vehiculos_PESO", "ve_weight")
setnames(full, "vehiculos_LONGITUD", "ve_length")
setnames(full, "vehiculos_MOTOR", "ve_motor")
setnames(full, "vehiculos_CC", "ve_cc")
setnames(full, "vehiculos_CV", "ve_hp")
setnames(full, "vehiculos_NPLAZAS", "ve_nseats")
setnames(full, "vehiculos_ACELER", "ve_acc")
setnames(full, "vehiculos_FVENTA", "ve_sale_date")
full[, vehiculos_VIGOR := NULL]
setnames(full, "vehiculos_PFF", "ve_price")
setnames(full, "vehiculos_PVENTA", "ve_sale_price")
full[, vehiculos_FVIGCOE := NULL]

setnames(full, "vehiculos_MERCADO", "ve_market")
setnames(full, "vehiculos_GAMA", "ve_gama")
full[, vehiculos_MARK := NULL]
full[, vehiculos_MARCAB7 := NULL]
full[, vehiculos_MODELOB7 := NULL]
full[, vehiculos_VERSIONB7 := NULL]
full[, vehiculos_RESTO:= NULL]
full[, vehiculos_ASTERISCO:= NULL]
full[, vehiculos_TIPOB7:= NULL]
full[, vehiculos_CLASEB7:= NULL]
full[, vehiculos_SEGMB7:= NULL]
full[, vehiculos_CODVEH:= NULL]

setnames(full, "vehiculos_SEGMAXA", "ve_axaseg")
full[, Veh_Cla_Agr := NULL]
setnames(full, "Veh_Ant", "ve_age")

full[, Veh_Cat := NULL]
full[, vehiculos_LCLASE:= NULL]
full[, poldirect_direct_year := NULL] ## puede q no este
full[, polaxa_polizas_year:= NULL] ## puede q no este 

## BODYSHOP
setnames(full, "provider_TAC", "garage_current_tac")
setnames(full, "provider_Nombre" , "garage_name")
full[, garage_name := gsub(";", "", garage_name)]

setnames(full, "provider_Via", "garage_address")
full[, garage_address := gsub(";", "", garage_address)]
setnames(full, "provider_Cod_Postal", "garage_zipcode")
setnames(full, "provider_Provincia", "garage_province")
setnames(full, "provider_Poblacion", "garage_city")
setnames(full, "provider_Tall_Marquista", "garage_brand_specialized")
setnames(full, "Tll_TAC", "garage_tac")
setnames(full, "provider_Categoria", "garage_category")
setnames(full, "Tll_Cat", "garage_category_id")
setnames(full, "Tll_TAC_Pre", "garage_tac_prev")
setnames(full, "provider_Fecha_Grabacion", "garage_date_record")
setnames(full, "provider_Fecha_Cat", "garage_date_category")
setnames(full,"Tll_RS_Mes", "garage_srra_month")
setnames(full, "Tll_RS_Ano", "garage_srra_year")
setnames(full, "Tll_Cod_Pro", "garage_province_code")
setnames(full, "Tll_PDg", "garage_with_expert")
setnames(full, "Tll_Grp", "garage_group")
setnames(full, "Tll_Srv_Ofi", "garage_official_service")
setnames(full, "Tll_Tip", "garage_type")   #"1 CRA, 2 CAMPA, 3 CENTRAL CHAPA, 4 CENTRO AVANCE, 0 SIN TIPO"
setnames(full, "Tll_Cod_SHP", "garage_sherpa_code")
setnames(full, "Tll_Val_SHP", "garage_sherpa")
setnames(full, "Tll_Acu_Id", "garage_special_agreement")
setnames(full, "Tll_Fec_Dto_Act", "garage_date_discount_update")
setnames(full, "Tll_Fec_Pre_Act", "garage_date_price_update")
full[, Ag_Acu_Tll_Id := NULL]
full[, "Tll_Nom_0":= NULL]
full[, provider_Tipo_Prov := NULL]
full[, Tll_Prf := NULL]
full[, provider_Profesion := NULL]
full[,provider_Estado_Prov := NULL]
full[,provider_Rep_Cristal_Puro := NULL]
full[,provider_provider_year:=NULL ]
full[, Rcl_Tll_Num := NULL]
setnames(full, "provider_Latitudes", "garage_lat")
setnames(full, "provider_Longitudes", "garage_lon")
full[, Tll_Cod_Pos := NULL]
full[, provider_Tipo_Prov := NULL]
full[, provider_Estado_Prov:=NULL]

full[, provider_provider_year:= NULL]
full[, garage_date_record:= NULL]
full[, garage_date_category := NULL]

## EXPERT
setnames(full, "CdPerito", "expert_code")
setnames(full, "NmHomoCh" , "expert_nhours_sheet")
setnames(full, "NumHorCh", "expert_nhours_sheet_cor")
setnames(full, "NmHomoPt", "expert_nhours_paint")
setnames(full, "Rsp_Fra_CS", "expert_fraud_confirmed")
setnames(full, "Rsp_Fra_Id", "expert_fraud")
setnames(full, "Rsp_Fra_Tip", "expert_fraud_type")
setnames(full, "Enc_Gar_RC", "expert_guarantee_rc")
setnames(full, "Enc_Gar_Agr", "expert_guarantee_group")
setnames(full, "Rsp_Imp_Tra_2", "expert_cost_section_b")
setnames(full, "Rsp_Imp_Tra", "expert_cost_section_a")
setnames(full, "Pag_Per_Imp", "expert_salary")
setnames(full, "Pag_Per_Fec", "expert_payment_date")
setnames(full, "Per_Val_SHP", "expert_sherpa")
setnames(full, "Enc_Can_Ape", "expert_opening_channel")
setnames(full, "Enc_Seg_Ape", "expert_opening_segment")
full[, Rcl_Per_Num := NULL]
full[, Per_Zon := NULL]
full[,Per_Cod_SHP:=NULL]
setnames(full, "FecGrabEn", "expert_date_request_saving")
setnames(full, "FecEfect", "expert_date")
setnames(full, "FecPeritEn", "expert_date_visit")
setnames(full, "PDg_Imp_Per", "expert_digital_validation_cost")

## AGENT
setnames(full, "Ag_Cod", "agent_code")
setnames(full, "Ag_Med", "agent_medor_code")
setnames(full, "Ag_Ofi", "agent_office_code")
setnames(full, "Ag_Reg", "agent_regional_code")    
full[, Ag_GCo := NULL]
full[, Ag_Ter := NULL]
full[, Ag_SAg := NULL]
setnames(full, "Ag_Niv_1", "agent_network_code")
setnames(full, "Ag_Seg_1", "agent_network_type")
setnames(full, "Ag_Niv_5", "agent_network_detail")    

## POLICY
full[, id_poliza := NULL]
setnames(full, "Pol_TAC","policy_tac_coverage")
setnames(full, "Pol_Gar", "policy_guarantee")                          
setnames(full, "Pol_Frq_Id", "policy_with_excess")                        
setnames(full, "Pol_Frq_Tp", "policy_excess_type")
setnames(full, "Pol_Fec_Or", "policy_initial_date")   	

##COST
setnames(full, "PreHorCh", "cost_hour_sheet")
setnames(full, "ImpMoCh" , "cost_sheet")
setnames(full, "ImpMoPt", "cost_paint")
setnames(full, "ImpMater", "cost_material")
setnames(full, "ImpMatPt", "cost_paint_material")
setnames(full, "ImpOtros", "cost_others")
setnames(full, "ImpNetrp", "cost_net")
setnames(full, "ImpDcTot", "cost_discount")
setnames(full, "ImpIvaPr", "cost_tax")
setnames(full, "Franquic", "cost_excess")
setnames(full, "ImpTotPr", "cost_total")
setnames(full, "PDg_Imp_GAP", "cost_gap_cross")
setnames(full, "Mat_Imp_DI", "cost_material_di") # puede q no este

## PARTS
setnames(full, "Mat_Num", "parts_number_replaced")
setnames(full, "Mat_Num_CZ", "parts_number_replaced_cz")
setnames(full, "Mat_Num_IP", "parts_number_replaced_ip")
setnames(full, "Mat_Imp_CZ", "parts_cost_replaced_cz") 
setnames(full, "Mat_Imp_IP", "parts_cost_replaced_ip") 
setnames(full, "Mat_Aho_CZ", "parts_saving_cz") 
setnames(full, "Mat_Aho_IP", "parts_saving_ip")
setnames(full, "MOb_Imp_Rep", "parts_cost_repaired")
setnames(full, "Aud_Par_Del_Rep", "parts_frontbumper_repaired")
setnames(full, "Aud_Par_Tra_Rep", "parts_backbumper_repaired")
setnames(full, "Aud_Pue_Rep", "parts_door_repaired")
setnames(full, "Aud_Cap_Rep", "parts_bonnet_repaired")
setnames(full, "Aud_Far_Rep", "parts_headlight_repaired")
setnames(full, "Aud_Ale_Rep", "parts_mudguard_repaired")
setnames(full, "Mat_Aho_Alt", "parts_saving_alternative")
full[, Mat_Num_DI := NULL]


#order
full[, Rea_Id := NULL]
setnames(full, "Rep_Id", "order_repaired")
full[, Enc_Id := NULL]
setnames(full, "Enc_Tipo", "order_type")
setnames(full, "TipEncar", "order_request_type")
setnames(full, "EstEncar", "order_status")
setnames(full, "FecEsten", "order_date")
setnames(full, "FecGrab", "order_date_answer")
#setnames(full, "Enc_Num_Web", "order_web_id")
setnames(full, "Enc_Val_Tip", "order_validation_type")
setnames(full, "PDg_Id", "order_digital_id")                                        
setnames(full, "Enc_Num_Tir", "order_tirea_id")  
setnames(full, "Enc_Num_Ori", "order_main_id")
setnames(full, "Enc_Web_Fin", "order_closed_webredes")

## TIME
setnames(full, "MOb_Hor_Rep", "time_reparation")
setnames(full, "FecPerit", "answer_date")
setnames(full, "TpResolu", "answer_type")
setnames(full, "Rep_Est_Id", "reparation_status")
setnames(full,  "CodGar" , "request_guarantee")    
setnames(full, "Val_Ide_Hos", "resolution_number_id")  


full[, vehiculos_FACTUALIZA := NULL]

full[, VehPerit := NULL]                                                       
full[, Enc_Compr := NULL]                                         
full[, PDg_Sol_PIS := NULL]
full[, PDg_Rsp_Par := NULL]
full[, PDg_Per_Vld := NULL]   
#full[, PDg_Imp_Tll := NULL] ## Coste del taller             
full[, Rsp_PT := NULL]
full[, Rsp_Fal := NULL]
full[, Rsp_Ppa := NULL]
full[, Enc_Cic_PRs := NULL]
full[, Enc_Cic_URs := NULL]
full[, Rep_Cic_DFn := NULL]
full[, Rep_Cic_IFn := NULL]
full[, Rep_Neg_IFn := NULL]                    
full[, Enc_Rea_Num := NULL ]
full[, Rsp_Mod_Num := NULL]
full[, Rsp_Sis_Cif := NULL]
full[, Rsp_Des_Rep := NULL]                              
full[, Rsp_Frq_Tck := NULL]
full[, PDg_Cic_Val := NULL]
full[, PDg_Cic_Par := NULL]
full[, Rsp_Grb_PFn := NULL]
full[, Sin_Ram := NULL]                                              
full[, Ag_Acu_Tll_Rd := NULL]                    
full[, Ag_Acu_Per_Id := NULL]
full[, Ag_Acu_Per_Rd := NULL]
full[, Cic_RS_Con := NULL]
full[, Cic_RS_Prv := NULL]
full[, Ag_Acu_Tll_Agr := NULL]
full[, Per_Rsp := NULL]
full[, PDg_Obj_Val := NULL]
full[, PDg_Obj_Par := NULL]
full[, Sin_Rec := NULL]
```


Now real feature engineering

```
full <- full[, order(colnames(full)), with=F]

get_mode <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}



full$agent_network_code <- as.factor(ifelse(full$agent_network_code == "1", "PROPIETARIA",
                                    ifelse(full$agent_network_code == "2", "NO PROPIETARIA", NA)))

full$agent_network_type=as.factor(ifelse(full$agent_network_type==1,"Agente AXA",
                              ifelse(full$agent_network_type==2,"Banco",
                              ifelse(full$agent_network_type==3,"Broker grande",
                              ifelse(full$agent_network_type==4,"Broker pequeÃ±o",NA)))))


full$answer_date <- as.Date(full$answer_date, "%d/%m/%Y")
full$claim_date_ending <- as.Date(full$claim_date_ending, "%d/%m/%Y")
full$claim_date_ocur <- as.Date(full$claim_date_ocur, "%d/%m/%Y")
full$claim_date_opening <- as.Date(full$claim_date_opening, "%d/%m/%Y")

full$claim_fraud_type <- as.factor(ifelse(full$claim_fraud_type=="O","Sin fraude",
                                   ifelse(full$claim_fraud_type=="A","Ficha fraude anulada",
                                    ifelse(full$claim_fraud_type=="C","Ficha fraude comunicacion pendiente",
                                    ifelse(full$claim_fraud_type=="F","Ficha fraude fallida",
                                    ifelse(full$claim_fraud_type=="N","Ficha fraude negativa",
                                    ifelse(full$claim_fraud_type=="R","Ficha fraude positiva",NA)))))))


full[claim_nature == "Colisi?n Objeto / Animal", claim_nature := "ColisiÃ³n Objeto / Animal"]
full[claim_nature == "Colisi?n m?s de un veh?culo", claim_nature := "ColisiÃ³n mÃ¡s de un vehÃ­culo"]
full[claim_nature == "Colisi?n otro vehiculo", claim_nature := "ColisiÃ³n otro vehiculo"]
full[claim_nature == "Da?os aparcamiento sin contrario", claim_nature := "DaÃ±os aparcamiento sin contrario"]
full[claim_nature == "Salida V?a", claim_nature := "Salida VÃ­a"]
full[claim_nature == "No colisi?n directa/ Colisi?n con otros objetos", claim_nature := "No colisiÃ³n directa/ ColisiÃ³n con otros objetos"]
full[claim_nature == "Da?os exclusivos lunas sin colisi?n con otro veh?c", claim_nature := "DaÃ±os exclusivos lunas sin colisiÃ³n con otro vehÃ­c"]
full[claim_nature == "Da?os derivados de robo", claim_nature := "DaÃ±os derivados de robo"]
full[claim_nature == "Confirmaci?n de inexistencia de siniestro (DIS)", claim_nature := "ConfirmaciÃ³n de inexistencia de siniestro (DIS)"]
full[claim_nature == "Sin Colisi?n M?s 1 Veh?culo", claim_nature := "Sin ColisiÃ³n MÃ¡s 1 VehÃ­culo"]
full[claim_nature == "Robo Total - Recuparado despu?s indem", claim_nature := "Robo Total - Recuparado despuÃ©s indem"]
full[claim_nature == "Da?os exclusivos lunas - por tentativa de robo", claim_nature := "DaÃ±os exclusivos lunas - por tentativa de robo"]


full[claim_opening_channel == "Autom?tica", claim_opening_channel := "AutomÃ¡tica"]
full[claim_opening_channel == "Front", claim_opening_channel := "Front Esp."]


full$claim_opening_type=as.factor(ifelse(full$claim_opening_type=="C","Corriente",
                                  ifelse(full$claim_opening_type=="R","Reapertura",
                                  ifelse(full$claim_opening_type=="T","Tardio",
                                  ifelse(full$claim_opening_type=="V","Variacion",NA)))))

full$claim_saving_fraud <- as.numeric(full$claim_saving_fraud)
full$cost_discount <- as.numeric(full$cost_discount)
full$cost_excess <- as.numeric(full$cost_excess)
full$cost_gap_cross <- as.numeric(full$cost_gap_cross)
full$cost_hour_sheet <- as.numeric(full$cost_hour_sheet)
full$cost_material <- as.numeric(full$cost_material)
full$cost_material_di <- as.numeric(full$cost_material_di)
full$cost_net <- as.numeric(full$cost_net)
full$cost_others <- as.numeric(full$cost_others)
full$cost_paint <- as.numeric(full$cost_paint)
full$cost_paint_material <- as.numeric(full$cost_paint_material)
full$cost_sheet <- as.numeric(full$cost_sheet)
full$cost_tax <- as.numeric(full$cost_tax)
full$cost_others <- as.numeric(full$cost_others)
full$cost_total <- as.numeric(full$cost_total)
full[, cost_nettax := cost_net + cost_tax]
full[, cost_nettaxdisc := cost_nettax - cost_discount]
full[, cost_nettaxdiscexc := cost_nettaxdisc - cost_excess]

full <- full[cost_nettax <= 3000,]
full[, cost_discount_percentage := cost_discount / cost_nettax]
full <- full[cost_discount_percentage < 0.25,]

full$expert_date <- as.Date(full$expert_date, "%d/%m/%Y")
full$expert_date_request_saving <- as.Date(full$expert_date_request_saving, "%d/%m/%Y")
full$expert_date_visit <- as.Date(full$expert_date_visit, "%d/%m/%Y")
full$expert_digital_validation_cost <- ifelse(full$expert_digital_validation_cost == ".", 0, as.numeric(full$expert_digital_validation_cost))

full$expert_fraud_type=as.factor(ifelse(full$expert_fraud_type==0,"No fraude",
                                   ifelse(full$expert_fraud_type==1,"Confirma sospecha",
                                    ifelse(full$expert_fraud_type==2,"Dudoso",
                                     ifelse(full$expert_fraud_type==3,"Elimina sospecha",NA)))))

full$expert_guarantee_group=as.factor(ifelse(full$expert_guarantee_group=="10","RC",
                                       ifelse(full$expert_guarantee_group=="20","DaÃ±os propios con franquicia",
                                        ifelse(full$expert_guarantee_group=="21","DaÃ±os propios sin franquicia",
                                       ifelse(full$expert_guarantee_group=="30","Lunas",
                                      ifelse(full$expert_guarantee_group=="40","Otros",NA))))))


full$expert_nhours_paint <- as.numeric(full$expert_nhours_paint)
full$expert_nhours_sheet <- as.numeric(full$expert_nhours_sheet)
full$expert_nhours_sheet_cor <- as.numeric(full$expert_nhours_sheet_cor)

full$expert_opening_channel=as.factor(ifelse(full$expert_opening_channel=="11","Automatico",
                                              ifelse(full$expert_opening_channel=="12","Automatico",
                                             ifelse(full$expert_opening_channel=="210","Front mexico",
                                             ifelse(full$expert_opening_channel=="211","Front mexico",
                                                   ifelse(full$expert_opening_channel=="220","Front spain",                                                                      
                                                ifelse(full$expert_opening_channel=="221","Front spain",
                                               ifelse(full$expert_opening_channel=="230","Direct",
                                          ifelse(full$expert_opening_channel=="31","Agente",
                                             ifelse(full$expert_opening_channel%in%c(51,52,53,59),"Middle",      
                                                ifelse(full$expert_opening_channel%in%c(40,41,42,43),"Otros",NA)))))))))))


full$expert_opening_segment=as.factor(ifelse(full$expert_opening_segment==1,"Automatico",
                                  ifelse(full$expert_opening_segment==2,"Front",
                                   ifelse(full$expert_opening_segment==3,"Agente",
                                    ifelse(full$expert_opening_segment==4,"Otros",
                                   ifelse(full$expert_opening_segment==5,"Middle",  NA))))))

full$expert_payment_date <- as.Date(full$expert_payment_date, "%d/%m/%Y")
full$expert_salary <- as.numeric(full$expert_salary)


full[which(is.na(expert_nhours_sheet_cor)), expert_nhours_sheet_cor := expert_nhours_sheet]
full[which(is.na(expert_opening_channel)), expert_opening_channel := "Otros"]

full$garage_date_discount_update <- as.Date(full$garage_date_discount_update, "%d/%m/%Y")
full$garage_date_price_update <- as.Date(full$garage_date_price_update, "%d/%m/%Y")

full$garage_type=as.factor(ifelse(full$garage_type==0,"Sin Tipo",
                         ifelse(full$garage_type==1,"CRA",
                            ifelse(full$garage_type==2,"CAMPA",
                           ifelse(full$garage_type==3,"Central chapa",
                        ifelse(full$garage_type==4,"Centro avance",  NA))))))
full[, Tll_Nom:= NULL]

full$mosaic_area <- as.numeric(full$mosaic_area)
full$mosaic_population <- as.numeric(full$mosaic_population)
aux <- mean(full$mosaic_area, na.rm = TRUE)
full[which(is.na(mosaic_area)), mosaic_area := aux]
aux <- mean(full$mosaic_population, na.rm = TRUE)
full[which(is.na(mosaic_population)), mosaic_population := aux]
aux <- get_mode(full$mosaic_code)
full[which(is.na(mosaic_code)), mosaic_code := aux]
aux <- get_mode(full$mosaic_code_det)
full[which(is.na(mosaic_code_det)), mosaic_code_det := aux]
aux <- get_mode(full$mosaic_georisk_av)
full[which(is.na(mosaic_georisk_av)), mosaic_georisk_av := aux]
aux <- get_mode(full$mosaic_georisk_gr)
full[which(is.na(mosaic_georisk_gr)), mosaic_georisk_gr := aux]

full$order_date <- as.Date(full$order_date, "%d/%m/%Y")
full$order_date_answer <- as.Date(full$order_date_answer, "%d/%m/%Y")

full$order_type=as.factor(ifelse(full$order_type=="1","Cara a cara",
                            ifelse(full$order_type=="2","Cara a cara",
                             ifelse(full$order_type=="6","Digital",
                              ifelse(full$order_type=="M1","Cara a cara multi",
                             ifelse(full$order_type=="M6","Digital multi",  NA))))))

full$parts_cost_avg <- as.numeric(full$parts_cost_avg)
full$parts_cost_repaired <- as.numeric(full$parts_cost_repaired)
full$parts_cost_replaced_cz <- as.numeric(full$parts_cost_replaced_cz)
full$parts_cost_replaced_ip <- as.numeric(full$parts_cost_replaced_ip)
full$parts_cost_total <- as.numeric(full$parts_cost_total)
full$parts_number <- as.numeric(full$parts_number)
full$parts_number_replaced <- as.numeric(full$parts_number_replaced)
full$parts_number_replaced_cz <- as.numeric(full$parts_number_replaced_cz)
full$parts_number_replaced_ip <- as.numeric(full$parts_number_replaced_ip)
full$parts_saving_alternative <- as.numeric(full$parts_saving_alternative)
full$parts_saving_cz <- as.numeric(full$parts_saving_cz)
full$parts_saving_ip <- as.numeric(full$parts_saving_ip)

full$policy_guarantee=as.factor(ifelse(full$policy_guarantee==0,"Desconocido",
                            ifelse(full$policy_guarantee==10,"RC",
                             ifelse(full$policy_guarantee==20,"DaÃ±os propios con franquicia",
                              ifelse(full$policy_guarantee==21,"DaÃ±os propios sin franquicia",
                               ifelse(full$policy_guarantee==9,"Desconocido",  "Desconocido"))))))

full$policy_initial_date <- as.Date(full$policy_initial_date, "%d/%m/%Y")

full$reparation_status=as.factor(ifelse(full$reparation_status==1,"Direccionado",
                                 ifelse(full$reparation_status==2,"Pendiente de inicio",
                                  ifelse(full$reparation_status==3,"En reparacion",
                                   ifelse(full$reparation_status==4,"Finalizado",
                                  ifelse(full$reparation_status==5,"Entregado", 
                                    ifelse(full$reparation_status==6,"Rechazado",NA)))))))

full$time_reparation <- as.numeric(full$time_reparation)
full$ve_age <- as.numeric(full$ve_age)
full$ve_hp <- as.numeric(full$ve_hp)
full$ve_weight <- as.numeric(full$ve_weight)
full$ve_ndoors <- as.numeric(full$ve_ndoors)
full$ve_price <- as.numeric(full$ve_price)
full$ve_nseats <- as.numeric(full$ve_nseats)
full$ve_length <- as.numeric(full$ve_length)
full$ve_cc <- as.numeric(full$ve_cc)
full$ve_acc <- as.numeric(full$ve_acc)

full[which(is.na(ve_brand)), ve_brand := "Desconocido"]

aux <- mean(full$ve_age, na.rm=TRUE)
full[is.na(ve_age), ve_age := aux]
aux <- mean(full$ve_ndoors, na.rm=TRUE)
full[is.na(ve_ndoors), ve_ndoors := aux]
aux <- mean(full$ve_hp, na.rm=TRUE)
full[is.na(ve_hp), ve_hp := aux]
aux <- mean(full$ve_weight, na.rm=TRUE)
full[is.na(ve_weight), ve_weight := aux]
aux <- mean(full$ve_price, na.rm=TRUE)
full[is.na(ve_price), ve_price := aux]
aux <- mean(full$ve_nseats, na.rm=TRUE)
full[is.na(ve_nseats), ve_nseats := aux]
aux <- mean(full$ve_length, na.rm=TRUE)
full[is.na(ve_length), ve_length := aux]
aux <- mean(full$ve_cc, na.rm=TRUE)
full[is.na(ve_cc), ve_cc := aux]
aux <- mean(full$ve_acc, na.rm=TRUE)
full[is.na(ve_acc), ve_acc := aux]

full[is.na(parts_number_replaced), parts_number_replaced := 0]
full[is.na(parts_number_replaced_cz), parts_number_replaced_cz := 0]
full[is.na(parts_number_replaced_ip), parts_number_replaced_ip := 0]
#full[is.na(parts_number), parts_number := 0]
#full[is.na(parts_cost_avg), parts_cost_avg := 0]
full[is.na(parts_cost_repaired), parts_cost_repaired := 0]
full[is.na(parts_saving_alternative), parts_saving_alternative := 0]
full[is.na(parts_saving_cz), parts_saving_cz := 0]
full[is.na(parts_saving_ip), parts_saving_ip := 0]
full[is.na(parts_cost_replaced_cz), parts_cost_replaced_cz := 0]
full[is.na(parts_cost_replaced_ip), parts_cost_replaced_ip := 0]
#full[is.na(parts_cost_total), parts_cost_total := 0]


full[is.na(cost_gap_cross), cost_gap_cross := 0]
full[is.na(cost_material_di), cost_material_di := 0]

full[, cost_sum_reparation := cost_sheet + cost_paint + cost_paint_material + cost_material]
full[, cost_ratio_sheet := ifelse(cost_sum_reparation != 0, cost_sheet / cost_sum_reparation, 0)]
full[, cost_ratio_painting := ifelse(cost_sum_reparation != 0, cost_paint / cost_sum_reparation, 0)]
full[, cost_material_total := ifelse(cost_sum_reparation != 0, cost_paint_material + cost_material, 0)]
full[, cost_ratio_material := ifelse(cost_sum_reparation != 0, cost_material_total / cost_sum_reparation, 0)]

full[, parts_number_alternative := parts_number_replaced_cz + parts_number_replaced_ip]
full[, parts_ratio_cz := ifelse(parts_number_alternative != 0, parts_number_replaced_cz/parts_number_alternative, 0) ]
full[, parts_ratio_ip := ifelse(parts_number_alternative != 0, parts_number_replaced_ip/parts_number_alternative, 0) ]
full[, parts_ratio_alternative := ifelse(parts_number_replaced != 0, parts_number_alternative/parts_number_replaced, 0)]

full[, claim_days_tillopen := as.numeric(difftime(claim_date_opening, claim_date_ocur, units = c("days")))]
full[, claim_month := month(claim_date_ocur)]
full[, claim_weekday := weekdays(claim_date_ocur)]

avg_price_brand <- full[, mean(ve_price), by = ve_brand]

brand_baratas <- avg_price_brand[V1 <= 15000,c("ve_brand"), with=FALSE]
brand_medias <- avg_price_brand[V1 > 15000 & V1 <= 25000,c("ve_brand"), with=FALSE]
brand_caras <- avg_price_brand[V1 > 25000 & V1 <= 40000,c("ve_brand"), with=FALSE]
brand_premium <- avg_price_brand[V1 > 40000 & V1 <= 60000,c("ve_brand"), with=FALSE]
brand_luxury <- avg_price_brand[V1 > 60000,c("ve_brand"), with=FALSE]
full[ve_brand %in% brand_baratas$ve_brand, ve_brand_category := "Barata"]
full[ve_brand %in% brand_medias$ve_brand, ve_brand_category := "Media"]
full[ve_brand %in% brand_caras$ve_brand, ve_brand_category := "Cara"]
full[ve_brand %in% brand_premium$ve_brand, ve_brand_category := "Premium"]
full[ve_brand %in% brand_luxury$ve_brand, ve_brand_category := "Lujo"]


full[, claim_nhours_type := mean(expert_nhours_sheet_cor), by = claim_type]
full[, claim_nhours_label := mean(expert_nhours_sheet_cor), by = claim_label]
full[, claim_materialcost_type := mean(cost_material), by = claim_type]
full[, claim_materialcost_label := mean(cost_material), by = claim_label]
```

Now Garage KPIs are computed:

```
full[, garage_kpi_ratio_cz := mean(parts_ratio_cz)  , by = garage_id]
full[, garage_kpi_ratio_ip := mean(parts_ratio_ip)  , by = garage_id]
full[, garage_kpi_ratio_alternative := mean(parts_ratio_alternative)  , by = garage_id]
full[, garage_kpi_nhours_sheet := mean(expert_nhours_sheet_cor), by=garage_id]
full[, garage_kpi_nhours_paint := mean(expert_nhours_paint), by=garage_id]
full[, garage_kpi_hour_sheet := mean(cost_hour_sheet), by = garage_id]
full[, garage_kpi_digital_assessment := sum(order_digital_id == "1")/.N, by = garage_id]
full[, garage_kpi_discountpercentage := sum(cost_discount)/sum(cost_nettax), by = garage_id]

avg_hour_sheet <- mean(full$cost_hour_sheet)
avg_hour_sheet_province <- full[, mean(cost_hour_sheet), by = garage_province]



for (i in 1:nrow(avg_hour_sheet_province)){
  prov <- avg_hour_sheet_province[i, c("garage_province"), with = FALSE]
  val <- avg_hour_sheet_province[i, c("V1"), with = FALSE]
  full[garage_province == prov, garage_kpi_hoursheet_province := garage_kpi_hour_sheet / val]
} 

full[, garage_kpi_hoursheet_national := garage_kpi_hour_sheet / avg_hour_sheet, by = garage_id ]
full[, garage_kpi_hoursheet_combined := garage_kpi_hoursheet_province * garage_kpi_hoursheet_national, by = garage_id]

full[, garage_kpi_hoursheet_province := as.numeric(garage_kpi_hoursheet_province)]
full[, garage_kpi_hoursheet_national := as.numeric(garage_kpi_hoursheet_national)]
full[, garage_kpi_hoursheet_combined := as.numeric(garage_kpi_hoursheet_combined)]

```

Up to this, **full_pay** has 326 columns and 363667 rows.
Column names are:
![colnames(full_1).PNG](#file:03b98853-eece-fa03-398f-8a25fe82f541)
![colnames(full_2).PNG](#file:1d43c630-437c-5e87-48c9-1d4ca49d83c3)

And finally first flat table, **full_pay** is created:

```
write.csv2(full, file = "~/full_pay_YYYYMMDD.csv" , row.names = FALSE, sep=";", quote = FALSE, fileEncoding = "ISO-8859-1")
```

Hence, final workflow for **full_pay** is:

## INSERT DATA FLOW IMAGE HERE


Afterwards, computing **bodyshops_pay** is straightforward, and next piece of code shows:

```
full <- full[order(-claim_date_opening),]

keep <- c("garage_address" , "garage_brand_specialized", "garage_category","garage_category_id","garage_city","garage_current_tac",
         #"garage_date_category",
		 "garage_date_discount_update","garage_date_price_update",#"garage_date_record"            
           "garage_group","garage_id","garage_kpi_digital_assessment","garage_kpi_hour_sheet"         
          , "garage_kpi_hoursheet_combined","garage_kpi_hoursheet_national" , "garage_kpi_hoursheet_province",  "garage_kpi_nhours_paint"       
          , "garage_kpi_nhours_sheet","garage_kpi_ratio_alternative",   "garage_kpi_ratio_cz","garage_kpi_ratio_ip"           
          , "garage_lat","garage_lon","garage_name","garage_official_service", "garage_kpi_discountpercentage"     
          , "garage_province","garage_province_code","garage_sherpa","garage_sherpa_code"            
          , "garage_special_agreement","garage_srra_month","garage_srra_year","garage_tac"                    
         ,"garage_tac_prev","garage_type","garage_with_expert","garage_zipcode" )

bodyshops <- full[, keep, with = F]
bodyshops <- unique(bodyshops, by = "garage_id")
#local path where csv is stored into Rstudio Environment
write.table(bodyshops, file = "~/bodyshops_pay_YYYYMMDD.csv" , row.names = FALSE, sep=";", quote = FALSE)
```
 
Flat table **bodyshop_pay** must have hence 37 column and 23457 rows.
Colum names are:
![colnames(bodyshops).PNG](#file:924c8710-e010-43b1-0d02-249893ad22b5)




----------
# **Abstract 2 - Severity and Norm Cost**
-------------

Due to the fact that two bodyshops around the country could charge huge differencies in a bill for the same claim, and both are, in a numerical point of view profitable. There is a need of normalize how much cost claims around the country, by understanding for instance that distinct location might explain differencies,  and brand and model needs different cares.
In order to create a complex rule to compare different bodyshops, there is next development, which goal is simple, how to find this rule to compare, and how could the company take advantage of this data in order to move claims flow to maximize savings, by knowing which bodyshop is better for each claim after knowing a bag of features, such as location, brand, model, kind of claim, etc.


Full model is written in **Python**. Scripts structure from src folder is, aligned to the execution stream: 

 1. **main.py**
 2. **bodyshop.py**
 3. **severity.py**
 4. **auxiliar.py**
 
In order to work properly, it is desirable to create a Python project and centralizing work flow, due to the difficulty of automatization in this situation.
Then, there is a fully explanation of each script, and how do they link each other, wondering to decrypt model streaming and functionalities.

 
----------


# **Main.py**
-------------

This script is written to have the chance of executing the whole model in a row. It coordinates functions, executions and  data **input** and **output**.

> **Script flow:**
>-
> - Imports pandas and other scripts.
> - Create paths for bodyshops and claims **datasets**.
> -  Reads datasets from paths by using **pandas dataframes**.
> -  Filters data by *cost_nettax* between (100, 3000), extracts formatted variables from *claim_year_month*, *cost_material*, *parts_number_replaced*, *expert_nhours_paint*, *expert_nhours_sheet_cor*.
>-  Afterwards, it computes minimun distance from each bodyshop to the nearest thousand. Once it computes this matrix, it stores information into *mat*. With this, there is no need to compute each time distances between bodyshops. By using *load_matrix* from **bodyshop.py**,  there is the chance of using your previously computed distances sparse matrix anytime.
> - Eventually, **main.py** calls severity funtion. First of all, it computes the predictive model, then it runs auxiliar *write_to_disk* function in order to save results in csv, and finally it fits model to the dataset, getting as a result severity for each claim.
> -  Finally, last step is computing cost normalization. In order to compute it, first it computes normalization of data and, as a final step, it runs *get_final_result*, in which function is computed normalized cost of each **claim** and each **bodyshop**. At the end of the script, it saves data into csv flat tables.

Main.py code is therefore:
```
import sys
sys.path.insert(0, "...") # local path where scripts are storaged


import pandas as pd
import bodyshop
import severity
import auxiliar
import norm_cost
import random
import numpy as np


DATE = "YYYYMMDD"

#########
## PATHS
#########
## Input
PATH = '...' #local path where full_pay and bodyshop_pay are storaged
BODY_PATH = PATH + 'bodyshops_pay_YYYYMMDD.csv'
CLAIM_PATH = PATH + 'full_pay_YYYYMMDD.csv'

## Output
CLOSEST_BODY_PATH = PATH + 'YYMMDD_distances_pay.pkl' #Matrix computed afterwards where distances are stored
CLAIM_SEVERITY = PATH + 'full_pay_YYYYMMDD.csv'


if __name__ == '__main__':

    bdo = pd.read_csv(BODY_PATH, delimiter=';', encoding="ISO-8859-1", low_memory=False)
    cdo = pd.read_csv(CLAIM_SEVERITY, delimiter='|', encoding="ISO-8859-1", low_memory=False)
    cdo = cdo.sample(frac=1).reset_index(drop=True)
    print "Data read"
    cdo = cdo[cdo['cost_nettax'] >= 100]
    cdo = cdo[cdo['cost_nettax'] < 3000]
    cdo['claim_year'] = cdo['claim_year_month'].astype(str).str[:4]

    cdo['cost_material_cut'] = pd.qcut(cdo['cost_material'], 3)
    cdo['parts_number_replaced_cut'] = pd.qcut(cdo['parts_number_replaced'], 3)
    cdo['expert_nhours_paint_cut'] = pd.qcut(cdo['expert_nhours_paint'], 3)
    cdo['expert_nhours_sheet_cor_cut'] = pd.qcut(cdo['expert_nhours_sheet_cor'], 3)
    print "Data formatted"
    print cdo.head()
    
    ## CLOSEST BODYSHOP
    #mat = bodyshop.all_distances(bdo, 1000)
    #bodyshop.store_matrix(mat, CLOSEST_BODY_PATH)
    mat = bodyshop.load_matrix(CLOSEST_BODY_PATH)
    print 'Matrix read'
    print mat.shape
    
    ## SEVERITY
    cdo = severity.predictive_severity(cdo)
    auxiliar.write_to_disk(cdo, CLAIM_SEVERITY)
    fit = severity.create_severity_model(cdo)
    print 'Severity end'

    ## NORM COST
    cost, cost_pred, st = norm_cost.normalization(cdo, bdo, CLOSEST_BODY_PATH)  # aqui está el error
    norm_cost.get_final_result(cost, cost_pred, st, bdo, cdo, date=DATE, nameaux='all_pay')
   
    print 'The End'
```


-------------

# **bodyshop.py**

-------------

Following libraries are imported:

```
import pandas as pd
import numpy as np
import scipy.sparse as sps
import pickle
from math import radians, cos, sin, asin, sqrt
```

There are 7 different functions in this script.

First is dist_haversine. Input,output and formula are:
Name: **haversine**
Input:
   - lon1: longitude of first coordinate
   - lat1: latitude of first coordinate
   - lon2: longitude of second coordinate
   - lat2: latitude of second coordinate
 Output: distance in kilometers
 Desc: Calculate distance in km between first and second coordinates
```
def haversine(lon1, lat1, lon2, lat2):
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km
```
Second is computing just one distance.  Input,output and formula are:

 Name: **one_distance**
 Input:
   - one: one row with longitude and latitude
   - position: position of case 'one' in 'body' (see next parameter)
   - body: pandas with many rows with lon and lat
   - number: number of closest rows to be kept in each case
   - lon: name of the longitude in pandas 'body' and row 'one'
   - lat: name of the latitude in pandas 'body' and row 'one'
 Output:
   - array with the 'number' closest distances in km
   - array with the 'number' closest positions in 'body'
 Desc: Calculate all the distances between one and all the rows in body. Only the 'number' closest are kept
```
def one_distance(one, position, body, number, lon='garage_lon', lat='garage_lat'):
    lon1, lat1 = one[lon], one[lat]
    aux = np.zeros(len(body))
    for j in range(len(body)):
        if position == j:
            aux[position] = 9999 # huge distance
        else:
            lon2, lat2 = body[lon].iloc[j], body[lat].iloc[j]
            aux[j] = haversine(lon1, lat1, lon2, lat2)
    aux_np = np.array(aux)
    pos = np.argsort(aux_np)[:number].tolist()
    return aux_np[pos], pos
```

Third is computing all distances. Input, output and formula are:
 Name: **all_distances**
 Input:
   - body: pandas with many rows with lon and lat
   - number: number of closest rows to be kept in each case
 Output:
   - sparse matrix with all the information related to closest rows to each other
 Desc: Calculate all the distances between all the rows in body, only the 'number' closest are kept in each case
```
def all_distances(body, number = 1000):
    mat = sps.coo_matrix((len(body), len(body)))
    mat = mat.tolil()
    for i in range(len(body)):
        res, pos = one_distance(body.iloc[i], i, body, number)
        mat[i, pos] = res
    return mat
```
Forth is a formula to store results in a matrix. Input, output and formula are:
 Name: **store_matrix**
 Input:
   - mat: sparse matrix, output of 'all_distances'
   - path: path where mat is stored
 Output:
   -
 Desc: Store mat in hdd
```
def store_matrix(mat, path):
    f = open(path, 'wb')
    pickle.dump(mat, f, -1)
    f.close()
```
Fifth is a formula to load a dist matrix. Input, output and formula are:
 Name: **load_matrix**
 Input:
   - path: path where mat was stored
 Output:
   - matrix with distances
 Desc: load mat from hdd
```
def load_matrix(path):
    f = open(path, 'rb')
    mat = pickle.load(f)
    f.close()
    return mat
```
Sixth is a formula to get closest bodyshop for each one. Input, output and formula are:
 Name: **get_closest_bodyshop**
 Input:
   - id: bodyshop id
   - bdo: bodyshop original pandas (used to create mat)
   - mat: closest bodyshop matrix
   - number: threshold number of closest bodyshops to be output
   - distance: threshold distance in kilometers
   - thres: limit of bodyshops to return
 Output:
   - vector of bodyshops id and vector of distances
 Desc: from the precalculated matrix, get the closest bodyshops to a given one
```
def get_closest_bodyshop(id, bdo, mat, number, distance):
    index = bdo[bdo['garage_id'] == id].index
    closest_bodyshop = bdo['garage_id'].loc[mat[index,].nonzero()[1]] # garages_id
    if len(closest_bodyshop > 0):
        distances = np.array(mat[index,:].data[0])
        if distance is not None:
            positions = np.where(distances < distance)[0]
            closest_bodyshop = closest_bodyshop.iloc[positions]
            distances = distances[positions]
        if number is not None:
            positions = distances.argsort()[:number]
            closest_bodyshop = closest_bodyshop.iloc[positions]
            distances = distances[positions]
        return closest_bodyshop, distances
    else:
        print "bodyshop %s does not have any other close bodyshop" % (id)
        return None, None
```
Finally last formula is made to get bodyshop province. Name and definition of formula is:
Name: **get_bodyshop_province**
```
def get_bodyshop_province(id, bdo, tac = True):
    province = bdo[bdo['garage_id'] == id]['garage_province'].values[0]
    bodyshops_output = bdo[bdo['garage_province'] == province]
    bodyshops_output = bodyshops_output[bodyshops_output['garage_current_tac'] == 1]['garage_id'].values
    return bodyshops_output
```

-------------

# **severity.py**

-------------

Following libraries are loaded in this script:

```
from sklearn.cluster import KMeans, AffinityPropagation, AgglomerativeClustering, DBSCAN
from sklearn.metrics import silhouette_samples, silhouette_score
import string
import pandas as pd
from sklearn.cross_validation import KFold
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
import auxiliar
```


There are 3 different functions in this script.

First is clustering severity. It computes a k-means clustering with few variables related to severity of claim. The reason of this is to find patterns of customers related to this fields and trying to treat them similar afterwards. Input, output and formula are:

 Name: **clustering_severity**
 Input:
   - data: pandas with data
   - range: array with range of number of clusters (ex: [3, 5, 9, 15, 20])
   - target: name of the target feature
 Others: There is a global variable with the features to be kept
 Output: -
 Desc: Clustering of data using the range of clusters. Silhouette validation is painted
```
def clustering_severity(data, range, target='cost_nettax'):
    cdo_cl = data[KEEP_SEV_CL]
    for n_cl in range:
        clusterer = KMeans(n_clusters=n_cl, random_state=10, init='k-means++')
        cluster_labels = clusterer.fit_predict(cdo_cl)
        name = 'severity_km_' + str(n_cl)
        data[name] = cluster_labels
        alphabet = list(string.ascii_lowercase)
        km_order_aux = data.groupby([name])[target].mean().order().index.values
        for i in range(len(km_order_aux)):
            data[name].replace(km_order_aux[i], alphabet[i], inplace=True)
        #a = cdo[name].value_counts()/len(cdo)
        #b = cdo.groupby([name])['cost_nettax'].mean()
        print(silhouette_score(cdo_cl, clusterer.labels_, metric='euclidean', sample_size=10000))
```

Second is a prediction of severity by using clusters computed with *clustering_severity*. Predictive RF model tries to choose best cluster to each client, based on sample severity clustering given before. It tries to reduce bias could appear by just computing minimun distance to centroids.

Input, output and formula are:
 Name: **predictive_severity**
 Input:
   - data: pandas with data
   - target: name of the target feature
   - k: number of folds for cross-validation
 Others: There are 2 global variables with the features to be kept (numerical y categorical)
 Output: pandas as input, plus one severity feature after cut
 Desc: RF is used to predict severity. After that, the output is cut based on the code and merged with the data
```
def predictive_severity(data, target='cost_nettax', k=10):
    model = RandomForestRegressor(n_estimators=150, n_jobs = 3, oob_score = True, max_features = 'sqrt', min_samples_leaf = 20)
    #model = GradientBoostingRegressor(n_estimators=100, learning_rate=0.1, max_depth=3,  min_samples_leaf=25)
    tdata = data[KEEP_SEV_PD_NUM]
    target = data[target]
    for col in KEEP_SEV_PD_CAT:
        tr_aux = pd.get_dummies(data[col], prefix=col)
        tdata = pd.concat([tdata, tr_aux], axis=1)
    names = tdata.columns
    cv = KFold(tdata.shape[0], indices=False, n_folds=k)
    features, predicted_list, data_predicted = list(), list(), list()
    for train_index, test_index in cv:
        xtr, xte = tdata.iloc[train_index], tdata.iloc[test_index]
        ytr, yte = target.iloc[train_index], target.iloc[test_index]
        fitted = model.fit(xtr, ytr)
        features.append(dict(zip(names, fitted.feature_importances_)))
        predicted = fitted.predict(xte)
        data_predicted.append(pd.DataFrame({'severity_model': predicted, 'severity_real': yte}, index=yte.index))
        predicted_list.append(predicted)
    data_predicted_all = pd.concat(data_predicted)
    measures = auxiliar.validate(data_predicted_all.severity_model, data_predicted_all.severity_real)
    feat = pd.DataFrame(features).mean().sort_values()
    print(measures)
    print(feat)

    data_predicted_all['severity_pm_10'] = auxiliar.cut_variable_volume(data_predicted_all['severity_model'].copy(), [6, 9, 12, 13, 20, 20, 8, 7, 3, 2])
    data_predicted_all = data_predicted_all.drop('severity_real', 1)
    data = data.merge(data_predicted_all, left_index=True, right_index=True, how='left')
    return(data)
```

Last funtion is **create_severity_model**. After running predictive_severity models,   it creates a prediction for *cost_nettax* used later for normalize each claim.
Input, output and formula are:
 Name: create_severity_model
 Input:
   - data: pandas with data
   - target: name of the target feature
 Output: fitted model.
 Desc: RF is used to predict severity. After that, the output is fitted model, trained in order to use it later on.

```
def create_severity_model(data, target='cost_nettax'):
    model = RandomForestRegressor(n_estimators=150, n_jobs = 3, oob_score = True, max_features = 'sqrt', min_samples_leaf = 20)
    tdata = data[KEEP_SEV_PD_NUM]
    target = data[target]
    for col in KEEP_SEV_PD_CAT:
        tr_aux = pd.get_dummies(data[col], prefix=col)
        tdata = pd.concat([tdata, tr_aux], axis=1)
    names = tdata.columns
    fitted = model.fit(tdata, target)
    return fitted
```

-------------

#  **norm_cost.py**

-------------

Following libraries are loaded in this script:

```
import auxiliar
import bodyshop
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.cross_validation import KFold
from collections import defaultdict
import math
from scipy import stats
```

First of all it is needed to define variables to be used around the model:

```
KEEP_NC_NUM = ['severity_pm_10','claim_days_tillopen', 'garage_kpi_hour_sheet',
               'garage_kpi_hoursheet_combined', 'garage_kpi_hoursheet_national', 'garage_kpi_hoursheet_province',
               'garage_kpi_ratio_alternative', 'garage_kpi_ratio_cz', 'garage_kpi_ratio_ip',
               'garage_official_service',  've_age',
               'claim_nhours_type', 'claim_nhours_label', 'claim_materialcost_type', 'claim_materialcost_label',
               've_hp', 've_sale_price', 've_weight', 've_cc', 've_length', 've_acc']

# expert_guarantee_group
KEEP_NC_CAT = ['claim_material', 'claim_type',
               'expert_opening_segment', 'garage_brand_specialized', 'garage_sherpa',
               'garage_tac', 'garage_type', 'garage_category', 'policy_guarantee', 've_brand_category',
               've_gama', 've_market', 've_axaseg', 've_motor']


GARAGE_ID = 'garage_id'
GARAGE_CURRENT_TAC = 'garage_current_tac'
```


Main weight of calculation is in this script. There are 19 different functions.

First function computes time weights for claims. Closest date in the data is set with the highest weight. 
Input, output and funtion are:
 Name: **create_weight**
 Input:
   - data: pandas with information of time features
   - feat: name of the time feature to calculate weigths
 Output: array of weights
 Desc: create time weights using a gaussian kernel. The closest date in the data is set with the highest weight
```
def create_weight(d, feat='claim_date_opening', form='%Y-%m-%d'):
    d[feat] = pd.to_datetime(d[feat], format=form)
    last_date = d[feat].max()
    weight_days = (last_date - d[feat]) / np.timedelta64(1, 'D')
    weight_days = weight_days.fillna(weight_days.mean())
    weights = np.array(auxiliar.kernel_norm(weight_days, weight_days.max()))
    return weights
```
Second function is to prepare data in order to ingest afterwards. Input, output and function are:

 Name: **prepare_data**
 Input:
   - data: pandas with all the information
   - target: target feature
   - log: boolean if the target feature must be used with the log function
 Others: there are two global arrays with the features to be kept (numerical and categorical)
 Output:
   - data with the features to be kept, codified as dummy features when necessary
   - column with the target feature
   - names of the data columns once all the features have been codified
 Desc: from the original data, only the features to be used in the model are kept. Besides, when these features  are categorical, they are translated into dummies.
```
def prepare_data(d, target='cost_nettaxdisc', log=True):
    if log:
        target = np.log(d[[target]])
    else:
        target = d[[target]]
    out = d[['garage_id', 'garage_current_tac'] + KEEP_NC_NUM]
    for col in KEEP_NC_CAT:
        tr_aux = pd.get_dummies(d[col], prefix=col)
        out = pd.concat([out, tr_aux], axis=1)
    names = out.columns
    return out, target, names
```

Third function split the array of names depending on case names are in an array of keys or not.

 Name: **separate_key_names**
 Input:
   - names: array with column names
   - keys: array with names to be separated from the first array
 Output:
   - names a is the array with names that are in keys
   - names b is the array with names that are NOT in keys
 Desc: split an array of names depending on whether the names are or not in an array of keys
```
def separate_key_names(names, keys=['garage']): # , 'mosaic' volver a poner en el array cuando acabe el cálculo
    m = []
    for k in keys:
        m += [n for n in names if k in n]
    names_a = [x for x in names if x in m]
    names_b = [x for x in names if x not in m]
    return names_a, names_b
```

Fourth function prepares bodyshop data in order to be ingested by the model. Inputm, output and function are:

 Name: **prepare_bodyshop_data**
 Input:
   - data: data after 'prepare_data'
   - names_bd: output a of 'separate_key_names'
 Output:
   - data prepared to be model
   - bodyshop information ready to be used in normalization
 Desc: this is a very ad-hoc function. Use wisely out of the normalizedd cost. It prepares the bodyshop information  from the data prepared to be model. Besides, it also cleans the data to be model

```
def prepare_bodyshop_data(data, names_bd):
    bdo_aux = data.drop_duplicates('garage_id')
    bdo_aux = bdo_aux[names_bd]
    names_bd.remove('garage_id')
    names_bd.remove('garage_current_tac')
    names_bd_pos_bdo_aux = []
    for name in names_bd:
        names_bd_pos_bdo_aux.append(bdo_aux.columns.values.tolist().index(name))
    names_bd_pos_data = []
    for name in names_bd:
        names_bd_pos_data.append(data.columns.values.tolist().index(name))
    data.drop('garage_id', axis=1, inplace=True)
    data.drop('garage_current_tac', axis=1, inplace=True)
    return data, bdo_aux, names_bd_pos_data, names_bd_pos_bdo_aux
```

Fifth function generates information of volumen and agreed for each bodyshop. Input, output and function are:

Name: **bodyshop_volume**
 Input:
   - data: original data
   - bdo: original bodyshop data
 Output:
   - pandas with volume and agreed information associated to each bodyshop
 Desc: generate information of volume and agreed for each bodyshop
```
def bodyshop_volume(data, bdo):
    body_vol = data['garage_id'].value_counts()
    body_vol = pd.DataFrame(data={'vol':body_vol.values}, index=body_vol.index)
    bdo_tac_info = bdo[['garage_id', 'garage_current_tac']]
    body_vol = body_vol.merge(bdo_tac_info, how='left', left_index=True, right_on='garage_id')
    body_vol = body_vol.drop_duplicates()
    return body_vol
```

Sixth function is an ad-hoc development to structure input data, model, result,  cross validation  and so on. Input, output and function are: 

 Name: **prepare_model**
 Input:
   - data: data to be model
 Output:
   - everything related to the model
 Desc: this is a very ad-hoc function where structure for result, model, cross-validation and so on are created
```
def prepare_model(data):
    model = GradientBoostingRegressor(n_estimators=200, learning_rate=0.1, max_depth=8,  min_samples_leaf=10)
   
    result = pd.DataFrame(data = {'norm_cost': np.zeros(len(data)), 'norm_cost_mea': np.zeros(len(data))}, index = data.index)
    
    result_pred = pd.DataFrame(data = np.zeros(len(data)), index = data.index, columns = ['predicted_cost'])
    cv = KFold(len(data), n_folds=2)
    
    st = pd.DataFrame(data = {'garage_id': np.zeros(len(data)), 'number': np.zeros(len(data)), 'tac': np.zeros(len(data)), 'notac': np.zeros(len(data)), 'dist': np.zeros(len(data))} , index = data.index)
    tag_flag = False
    weights = create_weight(data)
    return model, result, result_pred, cv, st, tag_flag, weights
```

Seventh function is one fold model, computed in order to predict values for test and getting information of FI and validation.
Input, output and function are:

 Name: **model_one_fold**
 Input:
   - data: data to be model
   - target: column with target feature
   - train: train indices
   - test: test indices
   - weights: array of weights
   - model:
   - log: boolean if the target feature is used with the log function
 Output: rf_fit, res and xte.
   -
 Desc: fit a model, predict values for test and get information of validation and feature importance
```
def model_one_fold(data, target, train, test, weights, model, log=True):
    xtr, xte = data.loc[data.index[train]], data.loc[data.index[test]]
    ytr, yte = target.loc[data.index[train]], target.loc[data.index[test]]
    wtr = weights[train]
    rf_fit = model.fit(xtr, ytr, sample_weight = wtr)
    if log:
        res = np.exp(rf_fit.predict(xte))
    else:
        res = rf_fit.predict(xte)
    print(auxiliar.validate(res, np.exp(yte)))
    auxiliar.print_feature_importance(xtr, rf_fit)
    #auxiliar.plot_feature_importance(xtr, rf_fit)
    return rf_fit, res, xte
```


Eigth function is a wrangling help in order to define data as an identification of bodyshop where each row was repaired. Input, output and function are:

 Name: **list_duplicates**
 Input:
   - seq: array with information
 Output:
   - tuple with the information ready to be appended to a dictionary
 Desc: this function is used to identify each row with the bodyshop were it was repaired.

```
def list_duplicates(seq):
    tally = defaultdict(list)
    for i,item in zip(seq.index, seq):
        tally[item].append(i)
    return ((key,locs) for key,locs in tally.items())
```


Ninth function defines a data dictionary with information for bodyshops. Values are indices of orders repaired.
Input, output and function are:

 Name: **get_bodyshop_groups**
 Input:
   - data: original data
   - indices: test indices
 Output:
   - dictionary with
 Desc: dictionary with information as follows key (bodyshop): values (indices of orders repaired)
 
```
def get_bodyshop_groups(data, indices):
    current_bds = data['garage_id'].loc[indices]
    di = {}
    for dup in sorted(list_duplicates(current_bds)):
        aux = {dup[0] : dup[1]}
        di.update(aux)
    return di
```


Tenth function updates data structure related to closest bodyshop in order to normalize.
Input, output and function are:

 Name: **update_st**
 Input:
   - st: data structure created for store some results
   - values: indices involved in the result
   - key: bodyshop id involved in the normalization
   - clo_bod = closest bodyshop involved in the normalization
 Output:
   - updated data structure
 Desc: update the data structure related to the closest bodyshop for normalization.
 
```
def update_st(st, values, key, clo_bod):
    st.loc[values, 'garage_id'] = key
    st.loc[values, 'number'] = len(clo_bod)
    st.loc[values, 'tac'] = len(clo_bod[clo_bod['garage_current_tac'] == 1])
    st.loc[values, 'notac'] = len(clo_bod[clo_bod['garage_current_tac'] == 0])
    st.loc[values, 'dist'] = clo_bod['distance'].max()
    return st
```


Eleventh function computes Confidence Interval of normalized cost and cost at bodyshop level, getting CI for each bodyshop aswell.
Input, output and function are:

 Name: **calculate_CI**
 Input:
   - cost: actual cost
   - ncost: normalized cost
   - ci: interval confidence
 Output:
   - pandas with information about bodyshops in terms of cost vs norm cost
 Desc: calculate everything related to the comparison between cost and normalized cost at bodyshop level,
   obtaining also the confidence intervals for each bodyshop
```
def calculate_CI(cost, ncost, ci=0.05):
    size = cost.shape[0]
    tval = stats.t.ppf(1-ci/2, size - 1)
    x1 = np.mean(cost)
    x2 = np.mean(ncost)
    x12 = x1 - x2

    s1 = np.var(cost)
    s2 = np.var(ncost)
    mse = (s1 + s2)/2
    s12 = math.sqrt((2*mse)/size)
    low = x12 - (tval * s12)
    high = x12 + (tval * s12)

    por = x12/x1*100
    score = (por*(size-1))/(high - low + 0.00000000001)
    return pd.Series([size, low, high, x1, x2, x12, por, score], index = ['volume', 'low', 'high', 'cost', 'norm_cost', 'diff', 'percentage', 'score'])
```

Twelfth function computes the **output of norm cost** and diff, by substracting targ minus norm cost.
Function is:

```
def generate_output_order(data, keep, pred, res_pred, targ):
    keep_com = ['claim_id', 'expert_order_id', 'order_web_id', 'garage_id', 'garage_tac', 'claim_year',
                targ, 'claim_company', 'garage_province', 'garage_current_tac', 'claim_year_month'] + keep
    keep_com= list(set(keep_com))
    output = data[keep_com]
    output = output.merge(res_pred, how = 'left', left_index = True, right_index = True)
    output = output.merge(pred, how = 'left', left_index = True, right_index = True)
    output['diff'] = output[targ] - output['norm_cost']
    #output = output.drop_duplicates()
    return output
```

Thirteenth function computes the **output of norm cost** and diff, for each **bodyshop**.
Function is:

```
def generate_output_bodyshop(out_ord, body, year, targ):
    if not year:
        keep = ['garage_id', 'garage_zipcode', 'garage_current_tac', 'garage_name', 'garage_brand_specialized']
        body = body[keep]
    # For all the company
        out_all = out_ord.groupby(['garage_id', 'garage_province']).apply(lambda x: calculate_CI(x[targ], x['norm_cost'])).reset_index()
        out_all = out_all.merge(body, how = 'left', on = 'garage_id')
        # Difference between Axa and Direct
        out_ent = out_ord.groupby(['garage_id','garage_province', 'claim_company']).apply(lambda x: calculate_CI(x[targ], x['norm_cost'])).reset_index() # 'garage_tac',
        out_ent = out_ent.merge(body, how = 'left', on = 'garage_id')
        final_order_all = ['garage_id', 'garage_name',  'garage_province', 'garage_brand_specialized',
                           'volume', 'cost', 'norm_cost', 'diff',
                           'percentage', 'low', 'high', 'score']

        final_order_ent = ['garage_id', 'garage_name', 'garage_province', 'garage_brand_specialized',
                           'claim_company',
                           'volume', 'cost', 'norm_cost',
                           'diff', 'percentage', 'low', 'high', 'score'] # 'garage_tac', 'garage_current_tac',
        return out_all[final_order_all].drop_duplicates(), out_ent[final_order_ent].drop_duplicates()
       
    else: # we aggregate by year for the moment
        keep = ['garage_id', 'garage_zipcode', 'garage_name']
        body = body[keep]
        out_all = out_ord.groupby(['garage_id', 'garage_province', 'garage_brand_specialized', 'claim_year']).apply(lambda x: calculate_CI(x[targ], x['norm_cost'])).reset_index()
        out_all = out_all.merge(body, how = 'left', on = 'garage_id')
        # Difference between Axa and Direct
        out_ent = out_ord.groupby(['garage_id', 'garage_province', 'claim_company', 'garage_brand_specialized', 'claim_year']).apply(lambda x: calculate_CI(x[targ], x['norm_cost'])).reset_index()

        out_ent = out_ent.merge(body, how = 'left', on = 'garage_id')
        final_order_all = ['garage_id', 'garage_name', 'garage_brand_specialized',
                           'garage_province', 'claim_year',
                           'volume',
                           'cost', 'norm_cost', 'diff', 'percentage', 'low', 'high']

        final_order_ent = ['garage_id', 'garage_name', 'garage_brand_specialized',
                           'garage_province', 'claim_year',
                           'claim_company',
                           'volume', 'cost', 'norm_cost', 'diff', 'percentage', 'low', 'high', 'score'] 
        return out_all[final_order_all].drop_duplicates(), out_ent[final_order_ent].drop_duplicates()
        
```


Fourteenth function is core in this process. It train and validates model by CV.
Name and function are:
Name **Normalization.**
```
def normalization(data, bdo, path_mat, thres=49):
    mat = bodyshop.load_matrix(path_mat) #if loaded, not required
    tdata, target, names = prepare_data(data)
    body_vol = bodyshop_volume(tdata, bdo)
    names_bd,_ = separate_key_names(names)
    tdata, bdo_aux, names_bd_pos_data, names_bd_pos_bdo_aux = prepare_bodyshop_data(tdata, names_bd)
    model, result, result_pred, cv, st, tag_flag, weights = prepare_model(data)
    for train_index, test_index in cv:
        rf_fit, res, xte = model_one_fold(tdata, target, train_index, test_index, weights, model, True)
        print "model created"
        partial_results = pd.DataFrame(np.empty((len(xte), thres+1)), index = xte.index)
        partial_results[:] = np.NAN
        partial_results.iloc[:, 0] = res
        di = get_bodyshop_groups(data, xte.index)
        for key, values in di.items():
            partial_results, st = normalize_one_claim(xte, bdo, bdo_aux, rf_fit, body_vol, key, values, mat, st, partial_results, names_bd_pos_data, names_bd_pos_bdo_aux, 25, 100, thres, False)
        result.loc[xte.index, 'norm_cost'] = partial_results.median(axis = 1).values
        result.loc[xte.index, 'norm_cost_mea'] = partial_results.mean(axis = 1).values
        result_pred.loc[xte.index, 'predicted_cost'] = res
    st = st.drop_duplicates('garage_id')
    return result, result_pred, st
```


In order to normalize each bodyshop, previously it is mandatory to compute each claim normalization. This process is computed by:
 Name: **normalize_one_claim**
 Input:
   - xte: test data to obtain prediction
   - bdo: bodyshop pandas data
   - bdo_aux: bodyshop auxiliar pandas with info for predicting
   - rf_fit: fitted model
   - body_vol: bodyshop info about volume and agreed/non agreed (output of 'bodyshop_volume')
   - key: bodyshop id
   - values: indices of claims repaired in the same bodyshop
   - mat: matrix with distances between bodyshops
   - st: pandas to store information (output of 'prepare_model')
   - partial_results: data structure to store partial results
   - tag_flag: boolean indicating if only consider agreed or not
   - dist = max threshold of distance in km
   - vol = min volume of repairs
   - thres = maximum number of bodyshops to normalize
 Output:
   - partial results with all the predicted costs, the normalized cost will be the average of each partial result
   - the data structure with information about the closest bodyshop and so on
 Desc: for a set on rows (all repaired in the same bodyshop), we calculate the normalized cost in the closest
   bodyshops depending on some parameters

```
def normalize_one_claim(xte, bdo, bdo_aux, rf_fit, body_vol, key, values, mat, st, partial_results, names_bd_pos_data, names_bd_pos_bdo_aux,  dist, vol, thres, tag_flag ):
    #val_gap = pd.DataFrame(data={'tac': np.zeros((len(values))), 'notac': np.zeros(len(values))})
    closest_bodyshop, distances = bodyshop.get_closest_bodyshop(key, bdo, mat, None, dist)
    if closest_bodyshop is not None:
        clo_bod = pd.DataFrame(data = {'garage_id':closest_bodyshop.values, 'distance': distances})
        clo_bod = clo_bod.drop_duplicates()
        clo_bod = clo_bod.merge(body_vol, how='left', on = 'garage_id')
        
        if len(clo_bod) > thres:
            clo_bod = clo_bod.sort_values('distance')
            clo_bod = clo_bod.iloc[:thres]
        st = update_st(st, values, key, clo_bod)
        data_predict = xte.loc[values]

        partial_results = get_partial_results(clo_bod['garage_id'], bdo_aux, data_predict, rf_fit, partial_results, values, xte, names_bd_pos_data,names_bd_pos_bdo_aux)
    return partial_results, st
```


Sixteenth funtion computes partial results with fitted values by model. Input, output and function is:

 Name: **get_partial_results**
 Input:
   - closest_bodyshop:list of closest bodyshop to normalize
   - bdo_aux: bodyshop auxiliar pandas with info for predicting
   - data_predict: data to predict
   - rf_fit: fitted model
   - partial_results: data structure to store results
   - values: indices involved in the normalization
   - xte: test data to predict
   - names_bd_pos_data: position of the columns associated to the bodyshop in the test data
   - names_bd_pos_bdo_aux: position of the columns associated to the bodyshop in the bodyshop pandas
 Output: partial results with the fitted values
 Desc: obtain all the partial results for a set of indices and for the closest bodyshop
 
```
def get_partial_results(closest_bodyshop, bdo_aux, data_predict, rf_fit, partial_results, values, xte, names_bd_pos_data,names_bd_pos_bdo_aux):
    if len(closest_bodyshop) > 0:
        bd_tdata = bdo_aux[bdo_aux['garage_id'].isin(closest_bodyshop)]
        if len(bd_tdata) > 0:
            bd_tdata_aux = pd.concat([bd_tdata]*len(data_predict), ignore_index=True).values
            data_predict_aux = data_predict.loc[np.repeat(data_predict.index.values, len(bd_tdata))].values
            data_predict_aux[:, names_bd_pos_data] = bd_tdata_aux[:, names_bd_pos_bdo_aux]
            data_predict_aux = pd.DataFrame(data_predict_aux, columns = xte.columns)
            fitted = pd.DataFrame(np.reshape(np.exp(rf_fit.predict(data_predict_aux)), (len(data_predict), len(bd_tdata))), index = values)
            partial_results.loc[values, map(lambda x: x+1, range(fitted.shape[1]))] = fitted.values
    return partial_results
```


Eighteenth function is related to **getting partial results**. Input, output and function are:

Name: get_partial_results
 Input:
   - closest_bodyshop:list of closest bodyshop to normalize
   - bdo_aux: bodyshop auxiliar pandas with info for predicting
   - data_predict: data to predict
   - rf_fit: fitted model
   - partial_results: data structure to store results
   - values: indices involved in the normalization
   - xte: test data to predict
   - names_bd_pos_data: position of the columns associated to the bodyshop in the test data
   - names_bd_pos_bdo_aux: position of the columns associated to the bodyshop in the bodyshop pandas
 Output: partial results with the fitted values
 Desc: obtain all the partial results for a set of indices and for the closest bodyshop

```
def get_partial_results(closest_bodyshop, bdo_aux, data_predict, rf_fit, partial_results, values, xte, names_bd_pos_data,names_bd_pos_bdo_aux):
    if len(closest_bodyshop) > 0:
        bd_tdata = bdo_aux[bdo_aux['garage_id'].isin(closest_bodyshop)]
        if len(bd_tdata) > 0:
            bd_tdata_aux = pd.concat([bd_tdata]*len(data_predict), ignore_index=True).values
            data_predict_aux = data_predict.loc[np.repeat(data_predict.index.values, len(bd_tdata))].values
            data_predict_aux[:, names_bd_pos_data] = bd_tdata_aux[:, names_bd_pos_bdo_aux]
            data_predict_aux = pd.DataFrame(data_predict_aux, columns = xte.columns)
            fitted = pd.DataFrame(np.reshape(np.exp(rf_fit.predict(data_predict_aux)), (len(data_predict), len(bd_tdata))), index = values)
            partial_results.loc[values, map(lambda x: x+1, range(fitted.shape[1]))] = fitted.values
    return partial_results

```


And last funtion **gets final result**, by computing all the normalizations, generating outputs and saving final data to csv.
It is computed by:

```
def get_final_result(result, result_pred, st, bdo, cdo, date, target='cost_nettaxdisc', data_path = 'C:\\projects\\cca\\results\\',nameaux='all_pay_cl'):
    cdo = cdo.merge(result, left_index=True, right_index=True, how='left')
    cdo.to_csv(data_path + '160226_cdo_all.csv', sep=";", index=False, encoding='utf-8')
    output_orders = generate_output_order(cdo, KEEP_NC_NUM+KEEP_NC_CAT, result, result_pred, target)
    output_bodyshops_all, output_bodyshops_entity = generate_output_bodyshop(output_orders, bdo, False, target)
    output_bodyshops_year_all, output_bodyshops_year_entity = generate_output_bodyshop(output_orders, bdo, True, target)


    output_orders.to_csv(data_path+date+'_orders_normcost_v2_'+nameaux+'.csv', sep = ";", index=False, encoding='utf-8')
    output_bodyshops_all.to_csv(data_path+date+'_bodyall_normcost_v2_'+nameaux+'.csv', sep = ";", index=False, encoding='utf-8')
    output_bodyshops_entity.to_csv(data_path+date+'_bodyentity_normcost_v2_'+nameaux+'.csv', sep = ";", index=False, encoding='utf-8')
    output_bodyshops_year_all.to_csv(data_path+date+'_bodyallyear_normcost_v2_'+nameaux+'.csv', sep = ";", index=False, encoding='utf-8')
    output_bodyshops_year_entity.to_csv(data_path+date+'_bodyentityyear_normcost_v2_'+nameaux+'.csv', sep = ";", index=False, encoding='utf-8')

    st = st.drop_duplicates('garage_id')
    st.to_csv(data_path+date+'_all_garagest_'+nameaux+'.csv', sep=";", index=False, encoding='utf-8')

```

Once **main.py** is ejecuted, output example data might be a .csv list must be generated in <i></i> **C:\projects\cca\results\nextlevel** like this:
1.   YYMMDD_cdo_all.csv
2.   YYMMDD_all_garagest_all_pay.csv
3.   YYMMDD_bodyall_normcost_v2_all_pay.csv
4.   YYMMDD_bodyallyear_normcost_v2_all_pay.csv
5.   YYMMDD_bodyentity_normcost_v2_all_pay.csv
6.   YYMMDD_bodyentityyear_normcost_v2_all_pay.csv
7.   YYMMDD_orders_normcost_v2_all_pay.csv

