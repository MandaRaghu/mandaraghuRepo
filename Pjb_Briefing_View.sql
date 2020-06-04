/****** Object:  View [dbo].[vw_PJB_Briefing]    Script Date: 11-15-2019 13:54:39 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALter VIEW [dbo].[vw_PJB_Briefing]
AS (
	SELECT		MAIN.WeekEndingDate,
			MAIN.Regional_VP_NAME,
			MAIN.[Operations_VP_Emp_Name],
			MAIN.Area_Supervisor_Name,
			MAIN.Area_Supervisor_Number,	
			MAIN.EMAIL_ADDRESS,
			MAIN.OVP_EMAIL_ADDRESS,
			MAIN.EmployeeName,
			MAIN.EmployeeNumber,
			--MAIN.General_Foreman_Name,
			MAIN.Monday, MAIN.Tuesday, MAIN.Wednesday, MAIN.Thursday, MAIN.Friday, MAIN.Saturday, MAIN.Sunday,
			MAIN.Status
FROM
			(SELECT		Timesheets.WeekEndingDate,
						coalesce(Emp.Area_Supervisor_Name,'') Area_Supervisor_Name,
						coalesce(Emp.Area_Supervisor_Number,'') Area_Supervisor_Number,
						coalesce(Emp.Operations_VP_EMP_NAME,'') Operations_VP_Emp_Name,
						coalesce(Emp.Regional_VP_NAME,'') Regional_VP_Name,
						EMP_EMAIL.EMAIL_ADDRESS,
						OVP_EMAIL.EMAIL_ADDRESS OVP_EMAIL_ADDRESS,
						Timesheets.EmployeeName ,
						TimeSheets.EmployeeNumber,
						--coalesce(Emp.GENERAL_FOREMAN_NAME,'') [General_Foreman_Name],
			
						coalesce(Timesheets.[Day 1],'') + Case When Coalesce(PreJob.[Day 1], '') = '' 
						THEN '' Else CASE WHEN COALESCE(Timesheets.[Day 1],'') = '' THEN '' ELSE ', ' END END + Coalesce(PreJob.[Day 1],'') [Monday],
						coalesce(Timesheets.[Day 2],'') + Case When Coalesce(PreJob.[Day 2], '') = '' 
						THEN '' Else CASE WHEN COALESCE(Timesheets.[Day 2],'') = '' THEN '' ELSE ', ' END END + Coalesce(PreJob.[Day 2],'') [Tuesday] ,
						coalesce(Timesheets.[Day 3],'') + Case When Coalesce(PreJob.[Day 3], '') = '' 
						THEN '' Else CASE WHEN COALESCE(Timesheets.[Day 3],'') = '' THEN '' ELSE ', ' END END + Coalesce(PreJob.[Day 3],'') [Wednesday] ,
						coalesce(Timesheets.[Day 4],'') + Case When Coalesce(PreJob.[Day 4], '') = '' 
						THEN '' Else CASE WHEN COALESCE(Timesheets.[Day 4],'') = '' THEN '' ELSE ', ' END END + Coalesce(PreJob.[Day 4],'') [Thursday] ,
						coalesce(Timesheets.[Day 5],'') + Case When Coalesce(PreJob.[Day 5], '') = '' 
						THEN '' Else CASE WHEN COALESCE(Timesheets.[Day 5],'') = '' THEN '' ELSE ', ' END END + Coalesce(PreJob.[Day 5],'') [Friday] ,
						coalesce(Timesheets.[Day 6],'') + Case When Coalesce(PreJob.[Day 6], '') = '' 
						THEN '' Else CASE WHEN COALESCE(Timesheets.[Day 6],'') = '' THEN '' ELSE ', ' END END + Coalesce(PreJob.[Day 6],'') [Saturday] ,
						coalesce(Timesheets.[Day 7],'') + Case When Coalesce(PreJob.[Day 7], '') = '' 
						THEN '' Else CASE WHEN COALESCE(Timesheets.[Day 7],'') = '' THEN '' ELSE ', ' END END + Coalesce(PreJob.[Day 7],'') [Sunday],			
			
						CASE WHEN COALESCE(PreJob.[Day 1],'') + COALESCE(PreJob.[Day 2],'') + COALESCE(PreJob.[Day 3],'') + COALESCE(PreJob.[Day 4],'') +
								  COALESCE(PreJob.[Day 5],'') + COALESCE(PreJob.[Day 6],'') + COALESCE(PreJob.[Day 7],'') = '' THEN  'No Job Briefings Submitted'
						ELSE CASE WHEN 
							 CASE WHEN (COALESCE(Timesheets.[Day 1],'') in ('TS','(Storm) TS') AND COALESCE(PreJob.[Day 1],'') like 'PJB%') OR  
										COALESCE(Timesheets.[Day 1],'') + COALESCE(PreJob.[Day 1],'') = '' THEN 0 ELSE 1 END +
							 CASE WHEN (COALESCE(Timesheets.[Day 2],'') in ('TS','(Storm) TS') AND COALESCE(PreJob.[Day 2],'') like 'PJB%') OR  
										COALESCE(Timesheets.[Day 2],'') + COALESCE(PreJob.[Day 2],'') = '' THEN 0 ELSE 1 END +			
							 CASE WHEN (COALESCE(Timesheets.[Day 3],'') in ('TS','(Storm) TS') AND COALESCE(PreJob.[Day 3],'') like 'PJB%') OR  
										COALESCE(Timesheets.[Day 3],'') + COALESCE(PreJob.[Day 3],'') = '' THEN 0 ELSE 1 END +
							 CASE WHEN (COALESCE(Timesheets.[Day 4],'') in ('TS','(Storm) TS') AND COALESCE(PreJob.[Day 4],'') like 'PJB%') OR  
										COALESCE(Timesheets.[Day 4],'') + COALESCE(PreJob.[Day 4],'') = '' THEN 0 ELSE 1 END +
							 CASE WHEN (COALESCE(Timesheets.[Day 5],'') in ('TS','(Storm) TS') AND COALESCE(PreJob.[Day 5],'') like 'PJB%') OR  
										COALESCE(Timesheets.[Day 5],'') + COALESCE(PreJob.[Day 5],'') = '' THEN 0 ELSE 1 END +					
							 CASE WHEN (COALESCE(Timesheets.[Day 6],'') in ('TS','(Storm) TS') AND COALESCE(PreJob.[Day 6],'') like 'PJB%') OR  
										COALESCE(Timesheets.[Day 6],'') + COALESCE(PreJob.[Day 6],'') = '' THEN 0 ELSE 1 END +
							 CASE WHEN (COALESCE(Timesheets.[Day 7],'') in ('TS','(Storm) TS') AND COALESCE(PreJob.[Day 7],'') like 'PJB%') OR  
										COALESCE(Timesheets.[Day 7],'') + COALESCE(PreJob.[Day 7],'') = '' THEN 0 ELSE 1 END = 0
							 THEN 'Good' Else
							 'No PJB Submitted On Day(s)  (' +
							 CASE WHEN  (COALESCE(Timesheets.[Day 1],'') in ('TS','(Storm) TS') AND COALESCE(PreJob.[Day 1],'') Not Like 'PJB%') THEN ' Mon' ELSE '' END +
							 CASE WHEN  (COALESCE(Timesheets.[Day 2],'') in ('TS','(Storm) TS') AND COALESCE(PreJob.[Day 2],'') Not Like 'PJB%') THEN ' Tue' ELSE '' END +
							 CASE WHEN  (COALESCE(Timesheets.[Day 3],'') in ('TS','(Storm) TS') AND COALESCE(PreJob.[Day 3],'') Not Like 'PJB%') THEN ' Wed' ELSE '' END +
							 CASE WHEN  (COALESCE(Timesheets.[Day 4],'') in ('TS','(Storm) TS') AND COALESCE(PreJob.[Day 4],'') Not Like 'PJB%') THEN ' Thu' ELSE '' END +
							 CASE WHEN  (COALESCE(Timesheets.[Day 5],'') in ('TS','(Storm) TS') AND COALESCE(PreJob.[Day 5],'') Not Like 'PJB%') THEN ' Fri' ELSE '' END +
							 CASE WHEN  (COALESCE(Timesheets.[Day 6],'') in ('TS','(Storm) TS') AND COALESCE(PreJob.[Day 6],'') Not Like 'PJB%') THEN ' Sat' ELSE '' END +
							 CASE WHEN  (COALESCE(Timesheets.[Day 7],'') in ('TS','(Storm) TS') AND COALESCE(PreJob.[Day 7],'') Not Like 'PJB%') THEN ' Sun' ELSE '' END	 
							 + '  )' 	END END 
						as	 [Status]   
			FROM
			(SELECT		WeekEndingDate,
						EmployeeName,
						EmployeeNumber,			
						case when [Day 1] = 2 THEN '(STORM) TS' ELSE case when [Day 1] = 1 THEN 'TS' ELSE '' END END [Day 1],
						case when [Day 2] = 2 THEN '(STORM) TS' ELSE case when [Day 2] = 1 THEN 'TS' ELSE '' END END [Day 2],
						case when [Day 3] = 2 THEN '(STORM) TS' ELSE case when [Day 3] = 1 THEN 'TS' ELSE '' END END [Day 3],
						case when [Day 4] = 2 THEN '(STORM) TS' ELSE case when [Day 4] = 1 THEN 'TS' ELSE '' END END [Day 4],
						case when [Day 5] = 2 THEN '(STORM) TS' ELSE case when [Day 5] = 1 THEN 'TS' ELSE '' END END [Day 5],
						case when [Day 6] = 2 THEN '(STORM) TS' ELSE case when [Day 6] = 1 THEN 'TS' ELSE '' END END [Day 6],
						case when [Day 7] = 2 THEN '(STORM) TS' ELSE case when [Day 7] = 1 THEN 'TS' ELSE '' END END [Day 7]
			FROM		(SELECT		DISTINCT
									TSH.WeekEndingDate, TSC.EmployeeName, TSC.EmployeeNumber, 
									'Day ' + cast(case when DATEPART(weekday,TSD.WorkDate) = 1 THEN 7 ELSE DATEPART(weekday,TSD.WorkDate) - 1 END as nvarchar) DayNumber,
									CASE WHEN left(right( rtrim(TSH.JobNumber) , 3),1) = '8' THEN 2 ELSE 1 END  TS
						
									FROM		dbo.EXT_TBL_T_TSHeader TSH,
												dbo.EXT_TBL_T_TSDetails TSD,
												dbo.EXT_TBL_T_TSCrew TSC
									LEFT JOIN	[dbo].EXT_TBL_M_EMPLOYEE_HIERARCHY EMP
									ON			TSC.EmployeeNumber = EMP.PERSON_NUMBER
									WHERE		TSH.WeekEndingDate between	convert(date,GETDATE() - (DATEPART(WEEKDAY,GETDATE()) + 5)) 
																			and convert(date,GETDATE() - (DATEPART(WEEKDAY,GETDATE()) - 1))
												AND	TSH.HdrID = TSD.HdrID
												AND	TSH.HdrID = TSC.HdrID AND TSC.CrewLeader = 1
												and TSH.JOBNUMBER <> 'Holiday/Vacation' and TSH.PikeTime <> 1 ) src
						pivot
									(max(TS)
									for DayNumber in ([Day 1],[Day 2],[Day 3],[Day 4],[Day 5],[Day 6],[Day 7])) piv) Timesheets
			LEFT JOIN
						(SELECT		Pre_Job_Pre.WeekEndingDate, 
			Pre_Job_Pre.EmployeeNumber,
			case when Pre_Job_Pre.[Day 1] = 1 THEN 
											  CASE WHEN PreJob_Late2.[Day 1] = 'PJB-Late' THEN 'PJB-Late' ELSE 'PJB' END ELSE '' END [Day 1],														 
			case when Pre_Job_Pre.[Day 2] = 1 THEN 
											  CASE WHEN PreJob_Late2.[Day 2] = 'PJB-Late' THEN 'PJB-Late' ELSE 'PJB' END ELSE '' END [Day 2],														
			case when Pre_Job_Pre.[Day 3] = 1 THEN 	
											  CASE WHEN PreJob_Late2.[Day 3] = 'PJB-Late' THEN 'PJB-Late' ELSE 'PJB' END ELSE '' END [Day 3],																								 
			case when Pre_Job_Pre.[Day 4] = 1 THEN 
											  CASE WHEN PreJob_Late2.[Day 4] = 'PJB-Late' THEN 'PJB-Late' ELSE 'PJB' END ELSE '' END [Day 4],														 
			case when Pre_Job_Pre.[Day 5] = 1 THEN 
										      CASE WHEN PreJob_Late2.[Day 5] = 'PJB-Late' THEN 'PJB-Late' ELSE 'PJB' END ELSE '' END [Day 5],												
			case when Pre_Job_Pre.[Day 6] = 1 THEN 
											  CASE WHEN PreJob_Late2.[Day 6] = 'PJB-Late' THEN 'PJB-Late' ELSE 'PJB' END ELSE '' END [Day 6],														
			case when Pre_Job_Pre.[Day 7] = 1 THEN 
											  CASE WHEN PreJob_Late2.[Day 7] = 'PJB-Late' THEN 'PJB-Late' ELSE 'PJB' END ELSE '' END [Day 7]														
FROM
			(SELECT		DISTINCT
						convert(date,GETDATE() - (DATEPART(WEEKDAY,GETDATE()) - 1)) WeekEndingDate						
						,BH.[CreatedBy] EmployeeNumber														
						,'Day ' + 
						cast(case	when DATEPART(weekday,CONVERT(VARCHAR(10),BH.Briefing_Date,101)) = 1 
									THEN 7 
									ELSE DATEPART(weekday,CONVERT(VARCHAR(10),BH.Briefing_Date,101))- 1 END	as nvarchar)  
						DayNumber
						,1 TS
			FROM		(SELECT			BCL.BRIEFING_EMP_NUMBER CREATEDBY, 
										BH.BRIEFING_Date
						 FROM			dbo.Ext_TBL_T_BRIEFING_HEADER BH,
										dbo.Ext_TBL_T_BRIEFING_CREW_LEADERS BCL
						 WHERE			BH.BRIEFING_ID = BCL.BRIEFING_ID
						 GROUP BY		BCL.BRIEFING_EMP_NUMBER, BH.BRIEFING_Date) BH


			WHERE		BH.Briefing_Date	between convert(date,GETDATE() - (DATEPART(WEEKDAY,GETDATE()) + 5)) 
											and convert(date,GETDATE() - (DATEPART(WEEKDAY,GETDATE()) - 1))
			) SRC
			
			pivot		(max(TS) for DayNumber in ([Day 1],[Day 2],[Day 3],[Day 4],[Day 5],[Day 6],[Day 7]))  Pre_Job_Pre	

LEFT JOIN
			(SELECT		PreJob_Late.WeekEndingDate, 
						PreJob_Late.EmployeeNumber,
					PreJob_Late.[Day 1],
					PreJob_Late.[Day 2],
					PreJob_Late.[Day 3],
					PreJob_Late.[Day 4],
					PreJob_Late.[Day 5],
					PreJob_Late.[Day 6],
					PreJob_Late.[Day 7]
					FROM (			
								SELECT		WeekEndingDate, 
											EmployeeNumber,
											case when [Day 1] = 1 THEN --'PJB' ELSE '' END [Day 1],
																 CASE WHEN Late = 0 THEN 'PJB' ELSE'PJB-Late' END ELSE '' END [Day 1],
											case when [Day 2] = 1 THEN --'PJB' ELSE '' END [Day 2],
																 CASE WHEN Late = 0 THEN 'PJB' ELSE'PJB-Late' END ELSE '' END [Day 2],
											case when [Day 3] = 1 THEN --'PJB' ELSE '' END [Day 3],
																 CASE WHEN Late = 0 THEN 'PJB' ELSE'PJB-Late' END ELSE '' END [Day 3],
											case when [Day 4] = 1 THEN --'PJB' ELSE '' END [Day 4],
																 CASE WHEN Late = 0 THEN 'PJB' ELSE'PJB-Late' END ELSE '' END [Day 4],
											case when [Day 5] = 1 THEN --'PJB' ELSE '' END [Day 5],
																  CASE WHEN Late = 0 THEN 'PJB' ELSE'PJB-Late' END ELSE '' END [Day 5],
											case when [Day 6] = 1 THEN --'PJB' ELSE '' END [Day 6],
																 CASE WHEN Late = 0 THEN 'PJB' ELSE'PJB-Late' END ELSE '' END [Day 6],
											case when [Day 7] = 1 THEN --'PJB' ELSE '' END [Day 7]
																 CASE WHEN Late = 0 THEN 'PJB' ELSE'PJB-Late' END ELSE '' END [Day 7]
								FROM

								(SELECT		DISTINCT
											convert(date,GETDATE() - (DATEPART(WEEKDAY,GETDATE()) - 1)) WeekEndingDate
						
											,BH.[CreatedBy] EmployeeNumber
											,case when left(BH.CreatedDate,10) <> left(BH.Briefing_Date, 10) Then 1 Else 0 END Late									
											,'Day ' + 
											cast(case	when DATEPART(weekday,CONVERT(VARCHAR(10),BH.Briefing_Date,101)) 
														= 1		THEN 7 
																ELSE DATEPART(weekday,CONVERT(VARCHAR(10),BH.Briefing_Date,101))- 1 END	as nvarchar)  
											DayNumber
											,1 TS
								FROM		(SELECT BCL.BRIEFING_EMP_NUMBER CREATEDBY, BH.BRIEFING_Date, min(BH.CreatedDate) CreatedDate
											 FROM dbo.Ext_TBL_T_BRIEFING_HEADER BH,
											 dbo.Ext_TBL_T_BRIEFING_CREW_LEADERS BCL
											  WHERE BH.BRIEFING_ID = BCL.BRIEFING_ID
											  GROUP BY BCL.BRIEFING_EMP_NUMBER, BH.BRIEFING_Date) BH

								Where		BH.Briefing_Date between convert(date,GETDATE() - (DATEPART(WEEKDAY,GETDATE()) + 5)) 
													and convert(date,GETDATE() - (DATEPART(WEEKDAY,GETDATE()) - 1))
													) SRC
								pivot		(max(TS)
											for DayNumber in ([Day 1],[Day 2],[Day 3],[Day 4],[Day 5],[Day 6],[Day 7])) piv) PreJob_Late
				WHERE	PreJob_Late.[Day 1] = 'PJB-Late' OR
								PreJob_Late.[Day 2] = 'PJB-Late' OR
								PreJob_Late.[Day 3]	= 'PJB-Late' OR
								PreJob_Late.[Day 4] = 'PJB-Late' OR
								PreJob_Late.[Day 5] = 'PJB-Late' OR
								PreJob_Late.[Day 6] = 'PJB-Late' OR
								PreJob_Late.[Day 7] = 'PJB-Late' 
								) PreJob_Late2

ON		Pre_Job_Pre.EmployeeNumber = PreJob_Late2.EmployeeNumber and Pre_Job_Pre.WeekEndingDate = PreJob_Late2.WeekEndingDate) PreJob
ON			right('00000' + Timesheets.EmployeeNumber,6) = right('00000' + PreJob.EmployeeNumber, 6)	
			LEFT JOIN	dbo.EXT_TBL_M_EMPLOYEE_HIERARCHY EMP
			ON			EMP.PERSON_NUMBER = Timesheets.EmployeeNumber
			LEFT JOIN	dbo.EXT_TBL_M_EMPLOYEE_HIERARCHY EMP_EMAIL
			ON			EMP.AREA_SUPERVISOR_NUMBER = EMP_EMAIL.PERSON_NUMBER
			LEFT JOIN	dbo.EXT_TBL_M_EMPLOYEE_HIERARCHY OVP_EMAIL
			ON			OVP_EMAIL.PERSON_NUMBER = EMP.OPERATIONS_VP_EMP_NUMBER
			WHERE		EMP.DEPARTMENT LIKE '%OH%' AND COALESCE(EMP.AREA_SUPERVISOR_NAME,'') <> '' AND EMP.JOB_CODE NOT LIKE '%GF%'
			) MAIN
--ORDER BY	MAIN.WeekEndingDate, MAIN.Area_Supervisor_Name, MAIN.EmployeeName,
--			CASE WHEN MAIN.Status = 'No Job Briefings Submitted' 
--			          THEN 300
--					  ELSE CASE WHEN MAIN.Status = 'Good' THEN 10 ELSE len(MAIN.Status) END END DESC
	);
GO


