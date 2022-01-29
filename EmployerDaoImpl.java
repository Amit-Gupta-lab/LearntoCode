package com.analyticsfox.dao;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

import javax.transaction.Transactional;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.hibernate.Criteria;

import org.hibernate.Query;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Order;
import org.hibernate.transform.AliasToBeanResultTransformer;
import org.hibernate.type.StandardBasicTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import com.analyticsfox.dto.DailyReportDto;
import com.analyticsfox.dto.DataAndCountDto;
import com.analyticsfox.dto.DemographicReportDto;
import com.analyticsfox.dto.DocumentNameStsDto;
import com.analyticsfox.dto.NameIdDto;
import com.analyticsfox.dto.SubForModuleDto;
import com.analyticsfox.dto.WeeklyReportStatusDto;
import com.analyticsfox.dto.WorkerDocListDto;
import com.analyticsfox.dto.WorkerDocMappingDto;
import com.analyticsfox.entity.ArchievedSubsEntity;
import com.analyticsfox.entity.ConfigurationEntity;
import com.analyticsfox.entity.DailyReportNotes;
import com.analyticsfox.entity.DailyReportNotesSubContractorDTO;
import com.analyticsfox.entity.DocLibMapping;
import com.analyticsfox.entity.DocumentLibrary;
import com.analyticsfox.entity.DocumentType;
import com.analyticsfox.entity.EmployerContactEntity;
import com.analyticsfox.entity.EmployerEntity;
import com.analyticsfox.entity.EmployerSubsMapping;
import com.analyticsfox.entity.EmployerUser;
import com.analyticsfox.entity.EmployerUserDOB;
import com.analyticsfox.entity.EmployerUserOptimized;
import com.analyticsfox.entity.InsuranceEntity;
import com.analyticsfox.entity.LibraryListForApp;
import com.analyticsfox.entity.ProjectEntity;
import com.analyticsfox.entity.ProjectWorkersMapping;
import com.analyticsfox.entity.RuleEntity;
import com.analyticsfox.entity.SkillSignalProfileEntity;
import com.analyticsfox.entity.Trades;
import com.analyticsfox.entity.WorkerDocMapping;
import com.analyticsfox.util.ConnectionFactory;
import com.analyticsfox.util.DateFormatConstants;

/**
 * This is a dao class for employer related CRUD operations
 * 
 * @author akkshay
 * @version 0.0.1
 *
 */
@Repository
@Transactional
public class EmployerDaoImpl implements EmployerDao {

	private static final Logger LOG = Logger.getLogger(EmployerDaoImpl.class.getName());

	@Autowired
	private SessionFactory sessionFactory;

	/**
	 * This method is to add or update Employer
	 * 
	 * @author akkshay
	 * @param EmployerEntity
	 * @return
	 * @throws Exception
	 * @since 01-06-2018
	 */
	@Override
	public EmployerEntity addOrUpdateEmployer(EmployerEntity employerEntity) throws Exception {
		return (EmployerEntity) sessionFactory.getCurrentSession().merge(employerEntity);
	}

	/**
	 * This method is to delete the Employer
	 * 
	 * @author akkshay
	 * @param id
	 * @return
	 * @throws Exception
	 * @since 05/06/2018
	 */
	@Override
	public String deleteEmployer(Long id) throws Exception {
		String queryString = "";
		String queryString1 = "";
		Query query = null;
		Query query1 = null;
		String status = null;
		// Checks if given employer is assign to any worker
		queryString = "select profile from SkillSignalProfileEntity profile where profile.employerEntity.id=:id";
		query = sessionFactory.getCurrentSession().createQuery(queryString).setParameter("id", id);
		List<SkillSignalProfileEntity> skillsignalEntity = query.list();
		queryString1 = "SELECT pwm.* FROM `project_workers_mapping` pwm INNER JOIN projects p ON p.id=pwm.site_id WHERE p.employer_id='"
				+ id + "'";
		query1 = sessionFactory.getCurrentSession().createSQLQuery(queryString1).addEntity(ProjectWorkersMapping.class);
		List<ProjectWorkersMapping> pwmList = query1.list();

		if (skillsignalEntity.isEmpty() && CollectionUtils.isEmpty(pwmList)) {
			try {
				queryString = "CALL DELETE_EMPLOYER(" + id + ")";
				query = sessionFactory.getCurrentSession().createSQLQuery(queryString);
				query.executeUpdate();
				status = "done";
			} catch (Exception ex) {
				LOG.error("Error occurred during DELETE_EMPLOYER" + ex);
			}
		} else {
			status = "assigned";
		}
		return status;
	}

	/**
	 * This method is to get Employer by id
	 * 
	 * @author akkshay
	 * @param id
	 * @return
	 * @throws Exception
	 * @since 01-06-2018
	 */
	@Override
	public EmployerEntity getEmployerById(Long id) throws Exception {
		LOG.info("getEmployerById >> employerId:" + id);
		final String queryString = "SELECT * FROM employer WHERE id=" + id;
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(EmployerEntity.class);
		return (EmployerEntity) query.uniqueResult();
	}

	/**
	 * This method is to get Employer by licenceNo
	 * 
	 * @author akkshay
	 * @param skillSignalId
	 * @return
	 * @throws Exception
	 * @since 09-06-2018
	 */
	public EmployerEntity getEmployerWithSubs(Long empId) throws Exception {

		String queryString = "";
		Query query = null;

		// Get all subs ids which has relation with given Employer
		queryString = "SELECT distinct subs_id FROM vw_subs WHERE id =" + empId + "";
		query = sessionFactory.getCurrentSession().createSQLQuery(queryString);
		List<BigInteger> subsIds = query.list();

		List<EmployerEntity> employerEntityList = new ArrayList<>();

		for (BigInteger subsId : subsIds) {
			queryString = "select * from employer where id =" + subsId;
			query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(EmployerEntity.class);
			EmployerEntity employerEntity = (EmployerEntity) query.uniqueResult();
			if (null != employerEntity)
				employerEntityList.add(employerEntity);
		}

		queryString = "select emp from EmployerEntity emp where emp.id =:empId";
		query = sessionFactory.getCurrentSession().createQuery(queryString).setParameter("empId", empId);
		EmployerEntity mainEmployerEntity = (EmployerEntity) query.uniqueResult();
		if (mainEmployerEntity != null) {
			employerEntityList = employerEntityList.stream()
					.sorted(Comparator.comparing(EmployerEntity::getEmployerName)).collect(Collectors.toList());
			mainEmployerEntity.setSubsList(employerEntityList);
		}
		return mainEmployerEntity;
	}

	//done Optimization of sub employer when getting hang in project module
	public EmployerEntity getEmployerWithSubsOptimized(Long empId) throws Exception {

		String queryString = "";
		Query query = null;

		// Get all subs ids which has relation with given Employer
		queryString = "SELECT distinct subs_id FROM vw_subs WHERE id =" + empId + "";
		LOG.info("query in getEmployerWithSubsOptimized "+queryString);
		query = sessionFactory.getCurrentSession().createSQLQuery(queryString);
		List<BigInteger> subsIds = query.list();

		List<EmployerEntity> employerEntityList = new ArrayList<>();

		//before optimization
		/*for (BigInteger subsId : subsIds) {
			queryString = "select * from employer where id =" + subsId;
			query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(EmployerEntity.class);
			EmployerEntity employerEntity = (EmployerEntity) query.uniqueResult();
			if (null != employerEntity)
				employerEntityList.add(employerEntity);
		}*/
		//done Optimization of sub employer when getting hang in project module
		try {
			LOG.info("time start "+new Timestamp(System.currentTimeMillis()));
			System.out.print("time start "+new Timestamp(System.currentTimeMillis()));
			
			if(!CollectionUtils.isEmpty(subsIds))
			{
				queryString = "select distinct id As id,employer_name As employerName from employer where id IN(:subsId)" ;
				query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
						.addScalar("id", StandardBasicTypes.LONG)
						.addScalar("employerName", StandardBasicTypes.TEXT).setParameterList("subsId", subsIds)
						.setResultTransformer(new AliasToBeanResultTransformer(EmployerEntity.class));
				employerEntityList=	query.list();
			}
			
			LOG.info("time end "+new Timestamp(System.currentTimeMillis()));
			System.out.print("time end "+new Timestamp(System.currentTimeMillis()));
		}
		catch(Exception e)
		{
			LOG.info("Exception ::"+e);
			e.printStackTrace();
		}

		queryString = "select emp from EmployerEntity emp where emp.id =:empId";
		query = sessionFactory.getCurrentSession().createQuery(queryString).setParameter("empId", empId);
		EmployerEntity mainEmployerEntity = (EmployerEntity) query.uniqueResult();
		if (mainEmployerEntity != null) {
			employerEntityList = employerEntityList.stream()
					.sorted(Comparator.comparing(EmployerEntity::getEmployerName)).collect(Collectors.toList());
			mainEmployerEntity.setSubsList(employerEntityList);
		}
		return mainEmployerEntity;
	}

	/**
	 * This method is to get Employer List (Active Only)
	 * 
	 * @author akkshay
	 * @return
	 * @throws Exception
	 * @since 01-06-2018
	 */
	@Override
	public List<EmployerEntity> getEmployerList() throws Exception {
		final String queryString = "SELECT * FROM employer ORDER BY employer_name ASC";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(EmployerEntity.class);
		return query.list();
	}

	/**
	 * This method is to get Subs List to add Subs
	 * 
	 * @author akkshay
	 * @return
	 * @param empSubsIdList
	 * @throws Exception
	 * @since 09-08-2018
	 */
	public List<NameIdDto> getNewEmployerList(long empId, Long siteId) throws Exception {

		// String queryString = "SELECT id AS id,employer_name AS `name` FROM
		// employer WHERE id NOT IN(SELECT subs_id FROM vw_subs WHERE id="
		// + empId + ") AND id<>" + empId + " ORDER BY `name` ASC";

		String queryString = "SELECT id AS id,employer_name AS `name` FROM employer WHERE id NOT IN(SELECT sub_id FROM project_sub_employer_mapping "
				+ " WHERE `site_id`=" + siteId + " AND `employer_id`=" + empId + "  AND sub_id IS NOT NULL) AND "
				+ " id<>" + empId + "  ORDER BY `name` ASC";

		@SuppressWarnings("unchecked")
		List<NameIdDto> employerList = sessionFactory.getCurrentSession().createSQLQuery(queryString)
				.addScalar("id", StandardBasicTypes.LONG).addScalar("name", StandardBasicTypes.TEXT)
				.setResultTransformer(new AliasToBeanResultTransformer(NameIdDto.class)).list();
		return employerList;
	}

	/**
	 * This method is to add or update Employer - Subs mapping
	 * 
	 * @author akkshay
	 * @param
	 * @return
	 * @throws Exception
	 * @since 12-06-2018
	 */
	@Override
	public void addOrUpdateEmployerSubsMapping(EmployerSubsMapping employerSubsMapping) throws Exception {
		sessionFactory.getCurrentSession().merge(employerSubsMapping);
	}

	/**
	 * This method is to delete the Employer - Subs Mapping
	 * 
	 * @author akkshay
	 * @param subsId
	 * @param skillsignalId
	 * @return
	 * @throws Exception
	 * @since 12-05-2018
	 */
	@Override
	public void deleteEmployerSubsMapping(String skillsignalId, Long subsId) throws Exception {
		final String queryString = "CALL DELETE_EMP_SUBS_MAPPING(:skillsignalId,:subsId)";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(ProjectEntity.class)
				.setParameter("skillsignalId", skillsignalId).setParameter("subsId", subsId);
		List<ProjectEntity> projects = query.list();

		new Thread(() -> {
			try {
				for (ProjectEntity project : projects) {
					List<String> ids = new ArrayList<String>(Arrays.asList(project.getSubList().split(",")));

					ids.remove(subsId.toString());
					String newIds = String.join(",", ids);
					project.setSubList(newIds);
					sessionFactory.getCurrentSession().saveOrUpdate(project);
				}
			} catch (Exception e) {
				// TODO: handle exception
			}
		}).start();
	}

	/**
	 * This method is to get insurance list for an Employer
	 * 
	 * @author akkshay
	 * @return
	 * @param empId
	 * @throws Exception
	 * @since 08-08-2018
	 */
	@Override
	public List<InsuranceEntity> getInsuranceList(Long empId) throws Exception {
		final String queryString = "select ins from InsuranceEntity ins where ins.employerId=:empId";
		Query query = sessionFactory.getCurrentSession().createQuery(queryString).setParameter("empId", empId);
		return query.list();
	}

	/**
	 * This method is to add or update Insurance
	 * 
	 * @author akkshay
	 * @param InsuranceEntity
	 * @return
	 * @throws Exception
	 * @since 08-08-2018
	 */
	public InsuranceEntity addOrUpdateInsurance(InsuranceEntity insuranceEntity) throws Exception {
		return (InsuranceEntity) sessionFactory.getCurrentSession().merge(insuranceEntity);
	}

	/**
	 * This method is to delete Insurance for an Employer
	 * 
	 * @author akkshay
	 * @param id
	 * @throws Exception
	 * @since 13-08-2018
	 */
	public void deleteInsurance(Long id) throws Exception {
		final String queryString = "delete from InsuranceEntity ins where ins.id=:id";
		Query query = sessionFactory.getCurrentSession().createQuery(queryString).setParameter("id", id);
		query.executeUpdate();
	}

	@Override
	public InsuranceEntity getInsuranceById(Long id) throws Exception {
		final String queryString = "select ins from InsuranceEntity ins where ins.id=:id";
		Query query = sessionFactory.getCurrentSession().createQuery(queryString).setParameter("id", id);
		return (InsuranceEntity) query.uniqueResult();
	}

	/**
	 * This method is to add or update Employer User
	 * 
	 * @author akkshay
	 * @param EmployerUser
	 * @return
	 * @throws Exception
	 * @since 25-09-2018
	 */
	@Override
	public EmployerUser addOrUpdateEmployerUser(EmployerUser employerUser) throws Exception {
		return (EmployerUser) sessionFactory.getCurrentSession().merge(employerUser);
	}

	/**
	 * This method is to get Employer User by Phone Number
	 * 
	 * @author akkshay
	 * @param phNumber
	 * @param licenceNo
	 * @return
	 * @throws Exception
	 * @since 25-09-2018
	 */
	public EmployerUser getEmployerUser(Long phNumber) throws Exception {
		final String queryString = "select usr from EmployerUser usr where usr.phoneNumber=:phoneNumber";
		Query query = sessionFactory.getCurrentSession().createQuery(queryString).setParameter("phoneNumber", phNumber);
		return (EmployerUser) query.uniqueResult();
	}

	/**
	 * This method is to get Employer User by Phone Number
	 * 
	 * @author akkshay
	 * @param id
	 * @return
	 * @throws Exception
	 * @since 14-11-2018
	 */
	public EmployerUser getEmployerUserById(Long id) throws Exception {
		LOG.info("Start of getEmployerUserById >> employerUserId : " + id);
		final String queryString = "select usr from EmployerUser usr where usr.id=:id";
		Query query = sessionFactory.getCurrentSession().createQuery(queryString).setParameter("id", id);
		return (EmployerUser) query.uniqueResult();
	}

	/**
	 * This method is to get all employer users list by state
	 * 
	 * @author akkshay
	 * @return
	 * @throws Exception
	 * @since 25-09-2018
	 */
	@Override
	public List<EmployerUser> getAllEmployerUsersList() throws Exception {
		final String queryString = "SELECT * FROM employer_user";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(EmployerUser.class);
		return query.list();
	}

	@Override
	public List<EmployerUser> getAllEmployerUserListLazy(Long offset, String searchChar) {
		StringBuilder sb = new StringBuilder(
				"SELECT `eu`.* FROM `employer_user` eu INNER JOIN `employer` e ON eu.`employer_id` = e.`id` ");
		if (!searchChar.isEmpty()) {
			sb.append(" WHERE eu.`phone_number` LIKE '%" + searchChar.replace(" ", "").replace(")", "").replace("(", "")
					+ "%' ");
			sb.append("OR CONCAT(eu.`first_name`,' ',eu.`last_name`) LIKE '%" + searchChar
					+ "%' OR e.employer_name LIKE '%" + searchChar + "%'");
		}
		sb.append(" ORDER BY CONCAT(eu.`first_name`,' ',eu.`last_name`) ");
		if (null != offset) {
			sb.append(" LIMIT 20 OFFSET " + offset);
		}
		Query query = sessionFactory.getCurrentSession().createSQLQuery(sb.toString()).addEntity(EmployerUser.class);
		return query.list();
	}

	/**
	 * This method is to get primary employer user only to show all employers in
	 * admin panel
	 * 
	 * @author akkshay
	 * @return
	 * @throws Exception
	 * @since May 8, 2019
	 */
	public List<EmployerUser> getAllEmployerList() throws Exception {
		final String queryString = "SELECT * FROM employer_user INNER JOIN employer ON employer_user.employer_id=employer.id "
				+ "WHERE employer_user.primary_user='Y' AND employer.is_active<>'inactive' "
				+ "GROUP BY primary_user,employer_id ORDER BY employer.employer_name ASC";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(EmployerUser.class);
		return query.list();
	}

	@Override
	public List<EmployerUser> getAllEmployerListLazy(Long offset, String searchChars) {
		StringBuilder sb = new StringBuilder(
				"SELECT `eu`.* FROM `employer_user` eu INNER JOIN `employer` e ON eu.`employer_id` = e.`id`");
		sb.append(" WHERE e.`is_active` <> 'inactive' AND eu.`primary_user` = 'Y'");

		if (!searchChars.isEmpty()) {
			sb.append(" AND (eu.`phone_number` LIKE '%" + searchChars.replace(" ", "").replace("(", "").replace(")", "")
					+ "%'");
			sb.append(" OR e.employer_name LIKE '%" + searchChars + "%')");
		}

		sb.append(" GROUP BY eu.`employer_id` ORDER BY e.employer_name");
		if (null != offset) {
			sb.append(" LIMIT 20 OFFSET " + offset);
		}
		Query query = sessionFactory.getCurrentSession().createSQLQuery(sb.toString()).addEntity(EmployerUser.class);
		return query.list();
	}

	/**
	 * This API is to get all employer user by employerId
	 * 
	 * @author akkshay
	 * @param empId
	 * @return
	 * @throws Exception
	 * @since Dec 19, 2018
	 */
	public List<EmployerUser> getEmployerUsersList(Long empId) throws Exception {
		final String queryString = "select usr from EmployerUser usr where usr.employerEntity.id=:empId "
				+ "order by usr.employerEntity.employerName asc";
		Query query = sessionFactory.getCurrentSession().createQuery(queryString).setParameter("empId", empId);
		return query.list();
	}

	/**
	 * This API is to get all employer user by licence number
	 * 
	 * @author akkshay
	 * @param licenceNo
	 * @return
	 * @throws Exception
	 * @since Dec 19, 2018
	 */
	public List<EmployerUserDOB> getEmployerUsersListForDataExtract(String licenceNo) throws Exception {
		final String queryString = "select usr from EmployerUserDOB usr where usr.employerDataDOB.concatLicenceNo=:licenceNo";
		Query query = sessionFactory.getCurrentSession().createQuery(queryString).setParameter("licenceNo", licenceNo);
		return query.list();
	}

	/**
	 * This method is to delete employer user
	 * 
	 * @author akkshay
	 * @param phoneNumber
	 * @throws Exception
	 * @since 25-09-2018
	 */
	public void deleteEmployerUser(Long phoneNumber) throws Exception {
		String queryString = "";
		Query query = null;

		queryString = "delete from user where role_id=2 and username=" + phoneNumber;
		query = sessionFactory.getCurrentSession().createSQLQuery(queryString);
		query.executeUpdate();

		queryString = "DELETE FROM employer_user WHERE phone_number=" + phoneNumber;
		query = sessionFactory.getCurrentSession().createSQLQuery(queryString);
		query.executeUpdate();
	}

	/**
	 * 
	 * Desc: This method is to get primary user of an employer
	 *
	 * @author akkshay
	 * @param empId
	 * @return
	 * @throws Exception
	 *             Sep 28, 2018
	 */
	@Override
	public EmployerUser getEmployerPrimaryUser(Long empId) throws Exception {
		final String queryString = "select usr from EmployerUser usr where usr.employerEntity.id=:empId and usr.primaryUser='Y'";
		Query query = sessionFactory.getCurrentSession().createQuery(queryString).setParameter("empId", empId)
				.setMaxResults(1);
		return (EmployerUser) query.uniqueResult();
	}
	
	//done Optimization of sub employer when getting hang in project module
	@Override
	public EmployerUserOptimized getEmployerPrimaryUserOptimized(Long empId) throws Exception {
		/*final String queryString = "select usr from EmployerUser usr where usr.employerEntity.id=:empId and usr.primaryUser='Y'";
		Query query = sessionFactory.getCurrentSession().createQuery(queryString).setParameter("empId", empId)
				.setMaxResults(1);*/
		try 
		{
			final String queryString = "SELECT e.id AS id,e.employer_name AS employerName,e.address1 AS address1,e.address2 AS address2,e.city AS city,e.state AS state, "
					+ "e.zip_code AS zipCode,e.latitude AS latitude,e.longitude AS longitude,eu.phone_number AS phoneNumber,eu.country_code AS countryCode "
					+ "FROM employer_user eu "
					+ "JOIN employer e ON eu.employer_id=e.id "
					+ "WHERE "
					+ "e.id=:empId "
					+ "AND "
					+ "eu.primary_user='Y' "
					+ "";
			Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
					.addScalar("id", StandardBasicTypes.LONG)
					.addScalar("employerName", StandardBasicTypes.TEXT)
					.addScalar("address1", StandardBasicTypes.TEXT)
					.addScalar("address2", StandardBasicTypes.TEXT)
					.addScalar("city", StandardBasicTypes.TEXT)
					.addScalar("state", StandardBasicTypes.TEXT)
					.addScalar("zipCode", StandardBasicTypes.TEXT)
					.addScalar("latitude", StandardBasicTypes.TEXT)
					.addScalar("longitude", StandardBasicTypes.TEXT)
					.addScalar("phoneNumber", StandardBasicTypes.LONG)
					.addScalar("countryCode", StandardBasicTypes.BIG_INTEGER)
					.setParameter("empId", empId)
					.setResultTransformer(new AliasToBeanResultTransformer(EmployerUserOptimized.class))
					.setMaxResults(1);
			return (EmployerUserOptimized) query.uniqueResult();

		}
		catch(Exception e)
		{
			LOG.info("Exception in getEmployerPrimaryUserOptimized::"+e);
			e.printStackTrace();
		}
		return new EmployerUserOptimized();
	}

	/**
	 * This API is to get EmployerList By Supervisor
	 * 
	 * @author priyanka
	 * @param supervisorId
	 * @return
	 * @throws Exception
	 * @since May 22, 2019
	 */
	public List<NameIdDto> getEmployerListBySupervisor(long supervisorId) throws Exception {
		final String queryString = "SELECT e.id as id,e.employer_name as name FROM employer e INNER JOIN crew c "
				+ "ON e.id=c.employer_id WHERE c.supervisor_worker_id=" + supervisorId;
		@SuppressWarnings("unchecked")
		List empList = sessionFactory.getCurrentSession().createSQLQuery(queryString)
				.addScalar("id", StandardBasicTypes.LONG).addScalar("name", StandardBasicTypes.TEXT)
				.setResultTransformer(new AliasToBeanResultTransformer(NameIdDto.class)).list();
		return empList;
	}

	/**
	 * This API is to Add Documentlib mapping
	 * 
	 * @author $ujanyaK
	 * @param listDocLibMapping
	 * @return
	 * @throws Exception
	 * @since May 22, 2019
	 */
	@Override
	public String addDocLibMapping(DocLibMapping listDocLibMapping) throws Exception {
		sessionFactory.getCurrentSession().merge(listDocLibMapping);
		return "done";
	}

	/**
	 * This return List of DocLibMapping data by docLibid
	 * 
	 * @author $ujanyaK
	 * @param docLibId
	 * @return
	 * @throws Exception
	 */
	@Override
	public List<DocLibMapping> getDocLibMapping(Long docLibId) throws Exception {
		final String queryString = "SELECT DISTINCT dlm.worker_id AS workerId, dlm.id AS id, dlm.doc_lib_id AS doc_lib_id, "
				+ "dlm.is_seen AS isSeen, dlm.seen_date AS seenDate ,\n"
				+ "	CONCAT(ssp.first_name, ' ', ssp.last_name) AS workerName\n"
				+ "FROM doc_lib_mapping dlm INNER JOIN skillsignal_profile ssp ON dlm.worker_id = ssp.id  \n"
				+ "WHERE dlm.employer_id <> 0 AND dlm.doc_lib_id=" + docLibId + "\n" + "GROUP BY dlm.worker_id";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
				.addScalar("workerId", StandardBasicTypes.LONG).addScalar("id", StandardBasicTypes.LONG)
				.addScalar("doc_lib_id", StandardBasicTypes.LONG).addScalar("isSeen", StandardBasicTypes.TEXT)
				.addScalar("workerName", StandardBasicTypes.TEXT).addScalar("seenDate", StandardBasicTypes.TEXT)
				.setResultTransformer(new AliasToBeanResultTransformer(DocLibMapping.class));
		return query.list();
	}

	/**
	 * This return List of DocLibMapping data by workerId
	 * 
	 * @author $ujanyaK
	 * @param docLibId
	 * @return
	 * @throws Exception
	 */
	@Override
	public List<DocLibMapping> getDocLibMappingByWorkerId(Long workerId) throws Exception {
		final String queryString = "SELECT * from  doc_lib_mapping  WHERE worker_id=" + workerId;
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(DocLibMapping.class);
		return query.list();
	}

	@Override
	public DocumentLibrary getDocumentLibByID(Long docLibId) throws Exception {
		final String queryString = "SELECT * FROM document_library  WHERE id=" + docLibId;
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(DocumentLibrary.class);
		return (DocumentLibrary) query.uniqueResult();
	}

	@Override
	public DocLibMapping getDocLibMappingById(Long id) throws Exception {
		final String queryString = "SELECT * from  doc_lib_mapping  WHERE id=" + id;
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(DocLibMapping.class);
		return (DocLibMapping) query.uniqueResult();

	}

	@Override
	public DocLibMapping getDocLibMappingByWorkerId(Long docLibId, Long workerId) throws Exception {
		final String queryString = "SELECT * FROM doc_lib_mapping  WHERE doc_lib_id=" + docLibId + " AND worker_id="
				+ workerId;
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(DocLibMapping.class);
		return (DocLibMapping) query.uniqueResult();
	}

	/**
	 * This method To GET Workers of employer
	 * 
	 * @$ujanyaK
	 * @param employerId
	 * @return
	 * @throws Exception
	 */

	@Override
	public List<SkillSignalProfileEntity> getAllWorkerByEmployerID(Long employerId) throws Exception {
		final String queryString = "SELECT * FROM `skillsignal_profile` WHERE `employer_id`=" + employerId
				+ " ORDER BY CONCAT(first_name,' ',last_name) ASC";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
				.addEntity(SkillSignalProfileEntity.class);
		return query.list();
	}

	@Override
	public List<BigInteger> getSubIdsBySkillSignal(String skillSignalId) throws Exception {
		final String queryString = "SELECT subs_id FROM employer_subs_mapping WHERE skillsignal_id='" + skillSignalId
				+ "'";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString);
		return query.list();
	}

	@Override
	public void addConfiguration(ConfigurationEntity configurationEntity) throws Exception {
		sessionFactory.getCurrentSession().merge(configurationEntity);
	}

	@Override
	public ConfigurationEntity getConfiguration(Long empusrId) throws Exception {
		final String queryString = "SELECT * FROM `configuration` WHERE `emp_usr_id`=" + empusrId
				+ " AND `site_id` IS NULL;";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
				.addEntity(ConfigurationEntity.class);
		return (ConfigurationEntity) query.uniqueResult();
	}

	@Override
	public EmployerSubsMapping isMapped(Long subId, String skillsignalId) throws Exception {
		final String queryString = "SELECT * FROM employer_subs_mapping WHERE subs_id=" + subId
				+ " AND skillsignal_id='" + skillsignalId + "'";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
				.addEntity(EmployerSubsMapping.class);
		return (EmployerSubsMapping) query.uniqueResult();
	}

	@Override
	public List<Trades> getTradesList() throws Exception {
		LOG.info("getTradesList ");
		final String queryString = "SELECT * FROM trades";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(Trades.class);
		return query.list();
	}

	@Override
	public List<DocumentLibrary> getSubEmployerDocumentList(Long empId, Long subEmpId) {
		List<DocumentLibrary> subEmployerDocumentList = sessionFactory.getCurrentSession()
				.createQuery(
						"FROM DocumentLibrary WHERE employerId = :empId AND subEmployerId = :subEmpId AND workerId = NULL")
				.setParameter("empId", empId).setParameter("subEmpId", subEmpId).list();
		return subEmployerDocumentList;
	}

	@Override
	public void deleteSubEmployerDocument(Long docId) throws Exception {
		sessionFactory.getCurrentSession().createQuery("delete from DocumentLibrary where id=:docId")
				.setParameter("docId", docId).executeUpdate();
	}

	@Override
	public List<EmployerEntity> getSelectedEmployerList(List<Long> employerIdList) {
		LOG.info("EmployerDaoImpl : getSelectedEmployerList >> ");
		Query query = sessionFactory.getCurrentSession()
				.createQuery(" FROM EmployerEntity emp WHERE id IN (:employerIdList) ORDER BY emp.employerName ASC");
		query.setParameterList("employerIdList", employerIdList);
		return query.list();
	}

	@Override
	public List<DocumentLibrary> getWorkerDocumentList(Long empId, Long workerId) throws Exception {
		LOG.info("getWorkerDocumentList>>");
		String queryString = "";
		Query query = null;

		if (null != empId) {
			queryString = "SELECT dl.* FROM `document_library` AS dl WHERE (dl.`employer_id`=" + empId
					+ " || dl.submitted_by IS NOT NULL) \r\n" + " AND dl.`worker_id`=" + workerId
					+ " AND dl.`sub_employer_id` IS NULL\r\n" + "ORDER BY dl.`created_date` DESC;";
			query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(DocumentLibrary.class);
		} else {
			queryString = "SELECT dl.*\n" + "FROM `document_library` AS dl\n" + "WHERE dl.`worker_id`=" + workerId
					+ " AND dl.`sub_employer_id` IS NULL \n" + "ORDER BY dl.`created_date` DESC;";
			query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(DocumentLibrary.class);
		}
		return query.list();
	}

	@Override
	public List<DocumentLibrary> getAssignedWorkerDocumentList(Long employerId, Long workerId) throws Exception {
		String queryString = "SELECT * FROM document_library WHERE employer_id=" + employerId
				+ " AND doc_type='d' AND `sub_employer_id` IS NULL AND worker_id IS NULL";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(DocumentLibrary.class);
		return query.list();
	}

	@Override
	public WorkerDocMapping getWorkerDocumentMappingById(long id) throws Exception {
		final String queryString = "select doc from WorkerDocMapping doc where doc.id =:id AND status NOT IN('Deleted')";
		Query query = sessionFactory.getCurrentSession().createQuery(queryString).setParameter("id", id);
		return (WorkerDocMapping) query.uniqueResult();
	}

	@Override
	public int deleteWorkerDocMapping(Long id) throws Exception {
		final String queryString = "DELETE FROM worker_doc_mapping WHERE id=" + id;
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString);
		return query.executeUpdate();
	}

	/**
	 * This method is used to get list of library for apps
	 * 
	 * @author Mayuri
	 * @param empId
	 * @param workerId
	 * @param docType
	 * @return LibraryListForApp
	 * @throws Exception
	 * @since 13-02-2019
	 */
	@Override
	public List<LibraryListForApp> getLibraryListForApp(Long empId, Long workerId, String docType) throws Exception {

		List<LibraryListForApp> libraryListForApps = new ArrayList<LibraryListForApp>();
		final String queryString = "SELECT DISTINCT dl.id, dl.`added_by`, dl.`document_description`, dl.`document_name`, dl.`document_path`, dlm.`is_seen`, dlm.`is_ack`, dl.thumbnail_path, \n"
				+ "	dlm.`seen_date` \n" + "FROM document_library dl \n"
				+ "INNER JOIN `doc_lib_mapping` dlm ON dl.id=dlm.`doc_lib_id` \n" + "WHERE dlm.`worker_id`=" + workerId
				+ " AND dl.`doc_type`='v' AND dl.`is_active` = TRUE "
				+ "ORDER BY dlm.is_seen, dlm.updated_date DESC";

		try (Connection con = (Connection) ConnectionFactory.getConnection("db1");
				PreparedStatement ps = (PreparedStatement) con.prepareStatement(queryString);) {
			try (ResultSet rs = ps.executeQuery();) {

				while (rs.next()) {

					LibraryListForApp libraryListForApp = new LibraryListForApp();
					libraryListForApp.setId(rs.getLong("id"));
					libraryListForApp.setDocTitle(rs.getString("document_name"));
					libraryListForApp.setDocDesc(rs.getString("document_description"));
					libraryListForApp.setDocLink(rs.getString("document_path"));
					libraryListForApp.setPublishedBy(rs.getString("added_by"));
					libraryListForApp.setIsAck(rs.getString("is_ack"));
					libraryListForApp.setIsSeen(rs.getString("is_seen"));
					libraryListForApp.setThumbnailPath(rs.getString("thumbnail_path"));
					libraryListForApp.setSeenDate(rs.getString("seen_date"));

					libraryListForApps.add(libraryListForApp);
				}
			} catch (Exception e) {
				LOG.info("Exception occured while getFromLibraryForApp : " + e);
			}
		} catch (Exception e) {
			LOG.info("Exception occured while getFromLibraryForApp : " + e);
		}

		return libraryListForApps;
	}

	@Override
	public List<LibraryListForApp> getLibraryListForWeb(Long empId, Long workerId, String docType) throws Exception {
		List<LibraryListForApp> libraryListForApps = new ArrayList<LibraryListForApp>();

		final String queryString = "SELECT DISTINCT dl.id, dl.`added_by`, dl.`document_description`, dl.`document_name`, dl.`document_path`, dlm.`is_seen`, dlm.`is_ack`, \n"
				+ "	dl.thumbnail_path, dlm.`seen_date` \n" + "FROM document_library dl \n"
				+ "INNER JOIN `doc_lib_mapping` dlm ON dl.id=dlm.`doc_lib_id` \n" + "WHERE dlm.`worker_id`=" + workerId
				+ " AND dl.`doc_type`='v' AND dl.`employer_id` IN (0, " + empId + ") AND dl.`is_active`=TRUE";

		try (Connection con = (Connection) ConnectionFactory.getConnection("db1");
				PreparedStatement ps = (PreparedStatement) con.prepareStatement(queryString);) {
			try (ResultSet rs = ps.executeQuery();) {

				while (rs.next()) {

					LibraryListForApp libraryListForApp = new LibraryListForApp();
					libraryListForApp.setId(rs.getLong("id"));
					libraryListForApp.setDocTitle(rs.getString("document_name"));
					libraryListForApp.setDocDesc(rs.getString("document_description"));
					libraryListForApp.setDocLink(rs.getString("document_path"));
					libraryListForApp.setPublishedBy(rs.getString("added_by"));
					libraryListForApp.setIsAck(rs.getString("is_ack"));
					libraryListForApp.setIsSeen(rs.getString("is_seen"));
					libraryListForApp.setThumbnailPath(rs.getString("thumbnail_path"));
					libraryListForApp.setSeenDate(rs.getString("seen_date"));
					// libraryListForApp.setDocActive(rs.getString("doc_active"));

					libraryListForApps.add(libraryListForApp);
				}
			} catch (Exception e) {
				LOG.info("Exception occured while getFromLibraryForApp : " + e);
			}
		} catch (Exception e) {
			LOG.info("Exception occured while getFromLibraryForApp : " + e);
		}

		return libraryListForApps;
	}

	/**
	 * This method is used to delete From Doc_Lib And Mapping
	 * 
	 * @author Mayuri
	 * @param docId
	 * @return
	 * @throws Exception
	 * @since 14-02-2019
	 */
	@Override
	public void deleteFromDocLibAndMapping(Long docId, String docType) throws Exception {

		String queryString1 = new String();
		String queryString3 = new String();
		try {
			// delete that doc from mapping
			if (StringUtils.equalsIgnoreCase(docType, "d")) {
				queryString1 = "DELETE FROM `worker_doc_mapping` WHERE doc_lib_id=" + docId
						+ " AND `status`IN('Assigned');";
				queryString3 = "DELETE FROM `project_doc_mapping` WHERE `doc_id`=" + docId + ";";
			} else if (StringUtils.equalsIgnoreCase(docType, "v")) {
				queryString1 = "DELETE FROM `doc_lib_mapping` WHERE `doc_lib_id`=" + docId + ";";
			}
			sessionFactory.getCurrentSession().createSQLQuery(queryString1).executeUpdate();
			if (StringUtils.isNotEmpty(queryString3))
				sessionFactory.getCurrentSession().createSQLQuery(queryString3).executeUpdate();

			// update flag to false of main doc/video
			final String queryString2 = "UPDATE `document_library` SET is_active=FALSE WHERE id=" + docId + "";
			Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString2);
			query.executeUpdate();
		} catch (Exception e) {
			e.printStackTrace();
			LOG.info("Exception in deleteFromDocLibAndMapping. " + e);
			for (int i = 0; i <= e.getStackTrace().length - 1; i++) {
				LOG.info(e.getStackTrace()[i].toString());
			}
		}
	}

	/**
	 * This API is to get subs list by EmpId
	 * 
	 * @author Mayuri
	 * @param empId
	 * @return subsList
	 * @throws Exception
	 * @since 17-02-2020
	 */
	@Override
	public List<NameIdDto> getSubsListByEmpId(String empId) throws Exception {
		String queryString = "SELECT e.id AS id, e.employer_name AS `name` FROM employer e INNER JOIN vw_subs sv ON e.id=sv.subs_id "
				+ "WHERE sv.id=" + empId + " ORDER BY name ASC;";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
				.addScalar("id", StandardBasicTypes.LONG).addScalar("name", StandardBasicTypes.TEXT)
				.setResultTransformer(new AliasToBeanResultTransformer(NameIdDto.class));
		return query.list();
	}

	@Override
	public boolean deleteEmployerContactById(Long id) throws Exception {
		Session session = sessionFactory.getCurrentSession();
		session.delete(session.get(EmployerContactEntity.class, id));
		return true;
	}

	@Override
	public DocumentLibrary addDocLibrary(DocumentLibrary docLibrary) throws Exception {
		return (DocumentLibrary) sessionFactory.getCurrentSession().merge(docLibrary);
	}

	@Override
	public List<DocumentLibrary> getDocumentLibraryByEmployerId(Long empId, String docType) throws Exception {
		String queryString = "SELECT * FROM document_library WHERE employer_id=" + empId + " AND doc_type='" + docType
				+ "' AND is_active=TRUE";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(DocumentLibrary.class);
		return query.list();
	}

	@Override
	public List<WorkerDocMappingDto> getAssignedDocuments(long empid, long workerid) throws Exception {
		/*
		 * final String queryString =
		 * "SELECT * FROM `worker_doc_mapping` WHERE `employer_id`=" + empid +
		 * " AND `worker_id`=" + workerid; Query query =
		 * sessionFactory.getCurrentSession().createSQLQuery(queryString).
		 * addEntity(WorkerDocMapping.class); return query.list();
		 */

		final String queryString = "SELECT wdm.id, wdm.`created_date` AS createdDate, wdm.`doc_lib_id` AS docLibId, wdm.`document_path` AS documentPath, wdm.`employer_id` \n"
				+ "	AS employerId, wdm.`status`, wdm.`updated_date` AS updatedDate, wdm.`worker_id` AS workerId, wdm.`assigned_date` AS assignedDate, wdm.submitted_by AS submittedBy, \n"
				+ "	p.`name` AS siteName, p.`employer_name` AS empNm, dl.`document_name` AS documentName, dl.`user_document_name` AS userDocumentName \n"
				+ "FROM `worker_doc_mapping` AS wdm \n" + "INNER JOIN `projects` AS p ON wdm.`site_id`=p.`id`\n"
				+ "INNER JOIN `document_library` AS dl ON wdm.`doc_lib_id`=dl.`id`\n" + "WHERE wdm.`employer_id`="
				+ empid + " AND wdm.`worker_id`=" + workerid + " AND wdm.`status` NOT IN('Deleted');";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
				.addScalar("id", StandardBasicTypes.LONG).addScalar("createdDate", StandardBasicTypes.TEXT)
				.addScalar("docLibId", StandardBasicTypes.LONG).addScalar("documentPath", StandardBasicTypes.TEXT)
				.addScalar("employerId", StandardBasicTypes.LONG).addScalar("status", StandardBasicTypes.TEXT)
				.addScalar("updatedDate", StandardBasicTypes.TEXT).addScalar("workerId", StandardBasicTypes.LONG)
				.addScalar("assignedDate", StandardBasicTypes.TEXT).addScalar("siteName", StandardBasicTypes.TEXT)
				.addScalar("empNm", StandardBasicTypes.TEXT).addScalar("documentName", StandardBasicTypes.TEXT)
				.addScalar("userDocumentName", StandardBasicTypes.TEXT)
				.addScalar("submittedBy", StandardBasicTypes.TEXT)
				.setResultTransformer(new AliasToBeanResultTransformer(WorkerDocMappingDto.class));
		return query.list();
	}

	@Override
	public List<RuleEntity> getRuleListByEmployer(Long employerId) {
		StringBuffer stringBuffer = new StringBuffer(
				"FROM RuleEntity ruleEntity WHERE ruleEntity.employerId =:employerId ");
		Query query = sessionFactory.getCurrentSession().createQuery(stringBuffer.toString());
		query.setParameter("employerId", employerId);
		return query.list();
	}

	@Override
	public void deleteRuleSet(Long ruleId) {
		StringBuffer stringBuffer = new StringBuffer("DELETE FROM rule WHERE id = :ruleId");
		Query query = sessionFactory.getCurrentSession().createSQLQuery(stringBuffer.toString()).setParameter("ruleId",
				ruleId);
		query.executeUpdate();
	}

	@Override
	public List<RuleEntity> getAllRuleSet(Long employerId, Long siteId) throws Exception {
		StringBuffer stringBuffer = new StringBuffer("FROM RuleEntity ");
		if (employerId == null) {
			stringBuffer.append("WHERE siteId= " + siteId);
		} else {
			stringBuffer.append("WHERE employerId= " + employerId);
		}
		Query query = sessionFactory.getCurrentSession().createQuery(stringBuffer.toString());
		return query.list();
	}

	@Override
	public List<DocumentLibrary> getFromLibrary(Long empId, String docType) {
		return sessionFactory.getCurrentSession()
				.createSQLQuery("select * from document_library where employer_id=" + empId + " AND doc_type='"
						+ docType + "' AND sub_employer_id IS NULL AND worker_id	IS NULL AND is_active=TRUE")
				.addEntity(DocumentLibrary.class).list();
	}

	@Override
	public HashMap<String, Set<Long>> getRuledCertificates(Long employerId, Long siteId, String isSite)
			throws Exception {

		HashMap<String, Set<Long>> map = new HashMap<String, Set<Long>>();
		Set<Long> HaveSet = new HashSet<Long>();
		Set<Long> FillSet = new HashSet<Long>();
		Set<Long> ViewSet = new HashSet<Long>();
		Set<Long> UpdateSet = new HashSet<Long>();

		String queryString = "SELECT `value_ids` , actions FROM rule WHERE `employer_id`=" + employerId + "";
		if (isSite.equalsIgnoreCase("Y")) {
			queryString = "SELECT `value_ids` , actions FROM rule WHERE `site_id`=" + siteId + "";
		}

		try (Connection con = ConnectionFactory.getConnection("db1");
				PreparedStatement ps = con.prepareStatement(queryString);) {
			try (ResultSet rs = ps.executeQuery();) {

				while (rs.next()) {
					if (StringUtils.equalsIgnoreCase(rs.getString("actions"), "Have")) {
						StringTokenizer tokenizer = new StringTokenizer(rs.getString("value_ids"), ",");
						while (tokenizer.hasMoreTokens()) {
							HaveSet.add(Long.parseLong(tokenizer.nextToken()));
						}
						map.put(rs.getString("actions"), HaveSet);
					}

					if (StringUtils.equalsIgnoreCase(rs.getString("actions"), "Fill")) {
						StringTokenizer tokenizer = new StringTokenizer(rs.getString("value_ids"), ",");
						while (tokenizer.hasMoreTokens()) {
							FillSet.add(Long.parseLong(tokenizer.nextToken()));
						}
						map.put(rs.getString("actions"), FillSet);
					}

					if (StringUtils.equalsIgnoreCase(rs.getString("actions"), "View")) {
						StringTokenizer tokenizer = new StringTokenizer(rs.getString("value_ids"), ",");
						while (tokenizer.hasMoreTokens()) {
							ViewSet.add(Long.parseLong(tokenizer.nextToken()));
						}
						map.put(rs.getString("actions"), ViewSet);
					}

					if (StringUtils.equalsIgnoreCase(rs.getString("actions"), "Update")) {
						StringTokenizer tokenizer = new StringTokenizer(rs.getString("value_ids"), ",");
						while (tokenizer.hasMoreTokens()) {
							UpdateSet.add(Long.parseLong(tokenizer.nextToken()));
						}
						map.put(rs.getString("actions"), UpdateSet);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				LOG.info("Exception occurred during getRuledCertificates. " + e);
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.info("Exception occurred during getRuledCertificates. " + e);
		}
		return map;
	}

	@Override
	public List<String> getDocNamesById(Set<Long> documentIds) throws Exception {

		List<String> docNameList = new ArrayList<String>();
		try {
			if (!CollectionUtils.isEmpty(documentIds)) {
				String docIds = documentIds.toString().replace("[", "").replace("]", "");
				String queryString = "SELECT DISTINCT dl.`user_document_name` FROM `worker_doc_mapping` AS wdm INNER JOIN `document_library` AS dl ON wdm.`doc_lib_id`=dl.`id` WHERE wdm.doc_lib_id IN("
						+ docIds + ") AND wdm.`status` NOT IN('Deleted');";
				Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString);
				docNameList = query.list();
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.info("Exception in getDocNamesById. " + e);
		}
		return docNameList;
	}

	@Override
	public List<NameIdDto> getVideoNamesById(Set<Long> videoIds) throws Exception {
		String vidIds = videoIds.toString().replace("[", "").replace("]", "");
		String queryString = "SELECT DISTINCT id, `document_name` AS name FROM `document_library` WHERE id IN(" + vidIds
				+ ") AND `employer_id`<>0;";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
				.addScalar("id", StandardBasicTypes.LONG).addScalar("name", StandardBasicTypes.TEXT)
				.setResultTransformer(new AliasToBeanResultTransformer(NameIdDto.class));
		;
		return query.list();
	}

	@Override
	public List<DocumentNameStsDto> getDocumentList(Long workerId, Long employerId) throws Exception {
		String queryString = "SELECT w.id, dl.`user_document_name` AS docName, w.status FROM `worker_doc_mapping` w \n"
				+ "INNER JOIN `document_library` AS dl ON w.`doc_lib_id`=dl.`id`\n" + "WHERE w.`worker_id`=" + workerId
				+ " AND w.`employer_id`=" + employerId + ";";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
				.addScalar("id", StandardBasicTypes.LONG).addScalar("docName", StandardBasicTypes.TEXT)
				.addScalar("status", StandardBasicTypes.TEXT)
				.setResultTransformer(new AliasToBeanResultTransformer(DocumentNameStsDto.class));
		return query.list();
	}

	@Override
	public List<DocumentNameStsDto> getVideoNamesByWorkerId(Long workerId) throws Exception {
		String queryString = "SELECT DISTINCT dl.id, dl.`document_name` AS docName, dlm.`is_seen` AS status\n"
				+ "FROM document_library dl \n" + "INNER JOIN `doc_lib_mapping` dlm \n" + "ON dl.id=dlm.`doc_lib_id` \n"
				+ "WHERE dlm.`worker_id`=" + workerId + " AND dl.`doc_type`='v' GROUP BY dl.`id`";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
				.addScalar("id", StandardBasicTypes.LONG).addScalar("docName", StandardBasicTypes.TEXT)
				.addScalar("status", StandardBasicTypes.TEXT)
				.setResultTransformer(new AliasToBeanResultTransformer(DocumentNameStsDto.class));
		return query.list();
	}

	@Override
	public DocLibMapping getDocLibMappingByEmployerId(Long workerId, Long employerId, Long documentId) {
		StringBuffer stringBuffer = new StringBuffer(
				"FROM DocLibMapping WHERE workerId = :workerId AND doc_lib_id = :documentId ");
		Query query = sessionFactory.getCurrentSession().createQuery(stringBuffer.toString())
				.setParameter("workerId", workerId).setParameter("documentId", documentId); // .setParameter("employerId",
																							// employerId)
		return (DocLibMapping) query.uniqueResult();
	}

	@Override
	public String addVideoMappingForAdmin(Long id, Long selectType) {
		String queryString = "CALL ADD_ADMIN_VIDEO_MAPPING(" + id + "," + selectType + ")";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString);
		query.executeUpdate();
		return "done";
	}

	@SuppressWarnings("unchecked")
	@Override
	public DataAndCountDto getSubsForSubsModule(Long empId, Long siteId, String isPriUser, Long employerUser,
			Long offset, String searchChars, Set<Long> subsIds, Set<Long> siteIds, Set<Long> allSubsIds,
			boolean isSubList) throws Exception {
		DataAndCountDto dataAndCountDto = new DataAndCountDto();
		try {
			StringBuffer queryString = new StringBuffer(
					"SELECT DISTINCT e.`id`,e.employer_name AS subName,e.address1,e.address2,e.city,e.state,e.zip_code AS zip,e.`latitude`,e.`longitude`,eu.phone_number AS phoneNo,eu.`country_code` AS countryCode ");
			String queryString2 = new String();
			if (siteId == 0) {
				if (null != isPriUser && isPriUser.equalsIgnoreCase("N")) {

					if (!CollectionUtils.isEmpty(siteIds) && !CollectionUtils.isEmpty(allSubsIds)) {
						if (!isSubList) {
							List<Long> archievedIds = getArchievedSubsIds(empId);
							if (!CollectionUtils.isEmpty(archievedIds)) {
								queryString2 = "SELECT DISTINCT e.`id`,e.subName,e.address1,e.address2,e.city,e.state,e.zip,e.`latitude`,e.`longitude`,e.phoneNo,e.countryCode\n"
										+ "FROM(\n"
										+ "SELECT DISTINCT e.`id`,e.employer_name AS subName,e.address1,e.address2,e.city,e.state,e.zip_code AS zip,e.`latitude`,e.`longitude`,\n"
										+ "	eu.phone_number AS phoneNo,eu.`country_code` AS countryCode \n"
										+ "FROM `project_key_person_entity` pkp \n"
										+ "INNER JOIN projects p ON pkp.`site_id`=p.`id` \n"
										+ "INNER JOIN `project_sub_employer_mapping` pke ON p.id=pke.site_id \n"
										+ "INNER JOIN employer e ON pke.sub_id = e.`id` \n"
										+ "INNER JOIN employer_user eu ON e.`id`=eu.`employer_id` \n"
										+ "WHERE pkp.ssid_access = TRUE AND pkp.`emp_user_id` = " + employerUser
										+ " AND p.`employer_id`=" + empId
										+ " AND eu.`primary_user` = 'Y' AND pkp.`site_id` IN("
										+ siteIds.toString().replace("[", "").replace("]", "")
										+ ")\n and e.`id` NOT IN("
										+ archievedIds.toString().replace("[", "").replace("]", "") + ") "
										+ "UNION ALL \n"
										+ "SELECT DISTINCT e.`id`,e.employer_name AS subName,e.address1,e.address2,e.city,e.state,e.zip_code AS zip,e.`latitude`,e.`longitude`,\n"
										+ "	eu.phone_number AS phoneNo,eu.`country_code` AS countryCode\n"
										+ "FROM `employer` AS e \n"
										+ "INNER JOIN employer_user AS eu ON e.`id`=eu.`employer_id`\n"
										+ "WHERE e.`id` IN(" + allSubsIds.toString().replace("[", "").replace("]", "")
										+ ") AND eu.`primary_user`='Y') AS e";
							} else {
								queryString2 = "SELECT DISTINCT e.`id`,e.subName,e.address1,e.address2,e.city,e.state,e.zip,e.`latitude`,e.`longitude`,e.phoneNo,e.countryCode\n"
										+ "FROM(\n"
										+ "SELECT DISTINCT e.`id`,e.employer_name AS subName,e.address1,e.address2,e.city,e.state,e.zip_code AS zip,e.`latitude`,e.`longitude`,\n"
										+ "	eu.phone_number AS phoneNo,eu.`country_code` AS countryCode \n"
										+ "FROM `project_key_person_entity` pkp \n"
										+ "INNER JOIN projects p ON pkp.`site_id`=p.`id` \n"
										+ "INNER JOIN `project_sub_employer_mapping` pke ON p.id=pke.site_id \n"
										+ "INNER JOIN employer e ON pke.sub_id = e.`id` \n"
										+ "INNER JOIN employer_user eu ON e.`id`=eu.`employer_id` \n"
										+ "WHERE pkp.ssid_access = TRUE AND pkp.`emp_user_id` = " + employerUser
										+ " AND p.`employer_id`=" + empId
										+ " AND eu.`primary_user` = 'Y' AND pkp.`site_id` IN("
										+ siteIds.toString().replace("[", "").replace("]", "") + ")\n" + "UNION ALL \n"
										+ "SELECT DISTINCT e.`id`,e.employer_name AS subName,e.address1,e.address2,e.city,e.state,e.zip_code AS zip,e.`latitude`,e.`longitude`,\n"
										+ "	eu.phone_number AS phoneNo,eu.`country_code` AS countryCode\n"
										+ "FROM `employer` AS e \n"
										+ "INNER JOIN employer_user AS eu ON e.`id`=eu.`employer_id`\n"
										+ "WHERE e.`id` IN(" + allSubsIds.toString().replace("[", "").replace("]", "")
										+ ") AND eu.`primary_user`='Y') AS e";
							}
						} else {
							queryString2 = "SELECT DISTINCT e.`id`,e.subName,e.address1,e.address2,e.city,e.state,e.zip,e.`latitude`,e.`longitude`,e.phoneNo,e.countryCode\n"
									+ "FROM(\n"
									+ "SELECT DISTINCT e.`id`,e.employer_name AS subName,e.address1,e.address2,e.city,e.state,e.zip_code AS zip,e.`latitude`,e.`longitude`,\n"
									+ "	eu.phone_number AS phoneNo,eu.`country_code` AS countryCode \n"
									+ "FROM `project_key_person_entity` pkp \n"
									+ "INNER JOIN projects p ON pkp.`site_id`=p.`id` \n"
									+ "INNER JOIN `project_sub_employer_mapping` pke ON p.id=pke.site_id \n"
									+ "INNER JOIN employer e ON pke.sub_id = e.`id` \n"
									+ "INNER JOIN employer_user eu ON e.`id`=eu.`employer_id` \n"
									+ "WHERE pkp.ssid_access = TRUE AND pkp.`emp_user_id` = " + employerUser
									+ " AND p.`employer_id`=" + empId
									+ " AND eu.`primary_user` = 'Y' AND pkp.`site_id` IN("
									+ siteIds.toString().replace("[", "").replace("]", "") + ")\n" + "UNION ALL \n"
									+ "SELECT DISTINCT e.`id`,e.employer_name AS subName,e.address1,e.address2,e.city,e.state,e.zip_code AS zip,e.`latitude`,e.`longitude`,\n"
									+ "	eu.phone_number AS phoneNo,eu.`country_code` AS countryCode\n"
									+ "FROM `employer` AS e \n"
									+ "INNER JOIN employer_user AS eu ON e.`id`=eu.`employer_id`\n" + "WHERE e.`id` IN("
									+ allSubsIds.toString().replace("[", "").replace("]", "")
									+ ") AND eu.`primary_user`='Y') AS e";
						}
					} else if (!CollectionUtils.isEmpty(siteIds) && CollectionUtils.isEmpty(allSubsIds)) {
						if (!isSubList) {
							List<Long> archievedIds = getArchievedSubsIds(empId);
							if (!CollectionUtils.isEmpty(archievedIds)) {
								queryString2 = "SELECT DISTINCT e.`id`,e.subName,e.address1,e.address2,e.city,e.state,e.zip,e.`latitude`,e.`longitude`,e.phoneNo,e.countryCode\n"
										+ "FROM(SELECT DISTINCT e.`id`,e.employer_name AS subName,e.address1,e.address2,e.city,e.state,e.zip_code AS zip,e.`latitude`,e.`longitude`,\n"
										+ "	eu.phone_number AS phoneNo,eu.`country_code` AS countryCode \n"
										+ "FROM `project_key_person_entity` pkp \n"
										+ "INNER JOIN projects p ON pkp.`site_id`=p.`id` \n"
										+ "INNER JOIN `project_sub_employer_mapping` pke ON p.id=pke.site_id \n"
										+ "INNER JOIN employer e ON pke.sub_id = e.`id` \n"
										+ "INNER JOIN employer_user eu ON e.`id`=eu.`employer_id` \n"
										+ "WHERE pkp.ssid_access = TRUE AND pkp.`emp_user_id` = " + employerUser
										+ " AND p.`employer_id`=" + empId
										+ " AND eu.`primary_user` = 'N' AND pkp.`site_id` IN("
										+ siteIds.toString().replace("[", "").replace("]", "") + ") and e.`id` NOT IN("
										+ archievedIds.toString().replace("[", "").replace("]", "") + ")) AS e";
							} else {
								queryString2 = "SELECT DISTINCT e.`id`,e.subName,e.address1,e.address2,e.city,e.state,e.zip,e.`latitude`,e.`longitude`,e.phoneNo,e.countryCode\n"
										+ "FROM(SELECT DISTINCT e.`id`,e.employer_name AS subName,e.address1,e.address2,e.city,e.state,e.zip_code AS zip,e.`latitude`,e.`longitude`,\n"
										+ "	eu.phone_number AS phoneNo,eu.`country_code` AS countryCode \n"
										+ "FROM `project_key_person_entity` pkp \n"
										+ "INNER JOIN projects p ON pkp.`site_id`=p.`id` \n"
										+ "INNER JOIN `project_sub_employer_mapping` pke ON p.id=pke.site_id \n"
										+ "INNER JOIN employer e ON pke.sub_id = e.`id` \n"
										+ "INNER JOIN employer_user eu ON e.`id`=eu.`employer_id` \n"
										+ "WHERE pkp.ssid_access = TRUE AND pkp.`emp_user_id` = " + employerUser
										+ " AND p.`employer_id`=" + empId
										+ " AND eu.`primary_user` = 'N' AND pkp.`site_id` IN("
										+ siteIds.toString().replace("[", "").replace("]", "") + ")) AS e";
							}
						} else {
							queryString2 = "SELECT DISTINCT e.`id`,e.subName,e.address1,e.address2,e.city,e.state,e.zip,e.`latitude`,e.`longitude`,e.phoneNo,e.countryCode\n"
									+ "FROM(SELECT DISTINCT e.`id`,e.employer_name AS subName,e.address1,e.address2,e.city,e.state,e.zip_code AS zip,e.`latitude`,e.`longitude`,\n"
									+ "	eu.phone_number AS phoneNo,eu.`country_code` AS countryCode \n"
									+ "FROM `project_key_person_entity` pkp \n"
									+ "INNER JOIN projects p ON pkp.`site_id`=p.`id` \n"
									+ "INNER JOIN `project_sub_employer_mapping` pke ON p.id=pke.site_id \n"
									+ "INNER JOIN employer e ON pke.sub_id = e.`id` \n"
									+ "INNER JOIN employer_user eu ON e.`id`=eu.`employer_id` \n"
									+ "WHERE pkp.ssid_access = TRUE AND pkp.`emp_user_id` = " + employerUser
									+ " AND p.`employer_id`=" + empId
									+ " AND eu.`primary_user` = 'N' AND pkp.`site_id` IN("
									+ siteIds.toString().replace("[", "").replace("]", "") + ")) AS e";
						}
					} else if (CollectionUtils.isEmpty(siteIds) && !CollectionUtils.isEmpty(allSubsIds)) {
						queryString2 = "SELECT DISTINCT e.`id`,e.subName,e.address1,e.address2,e.city,e.state,e.zip,e.`latitude`,e.`longitude`,e.phoneNo,e.countryCode\n"
								+ "FROM(SELECT DISTINCT e.`id`,e.employer_name AS subName,e.address1,e.address2,e.city,e.state,e.zip_code AS zip,e.`latitude`,e.`longitude`,\n"
								+ "	eu.phone_number AS phoneNo,eu.`country_code` AS countryCode\n"
								+ "FROM `employer` AS e \n"
								+ "INNER JOIN employer_user AS eu ON e.`id`=eu.`employer_id`\n" + "WHERE e.`id` IN("
								+ allSubsIds.toString().replace("[", "").replace("]", "")
								+ ") AND eu.`primary_user`='Y') AS e";
					}
				} else {
					if (!isSubList) {
						List<Long> archievedIds = getArchievedSubsIds(empId);
						if (!CollectionUtils.isEmpty(archievedIds)) {
							queryString.append("FROM vw_subs v INNER JOIN employer AS e ON v.`subs_id`=e.`id` ");
							queryString
									.append("INNER JOIN employer_user AS eu ON e.`id`=eu.`employer_id` WHERE eu.`primary_user`='Y' AND v.`id`="
											+ empId + " AND v.`subs_id` NOT IN("
											+ archievedIds.toString().replace("[", "").replace("]", "") + ")");
						} else {
							queryString.append("FROM vw_subs v INNER JOIN employer AS e ON v.`subs_id`=e.`id` ");
							queryString
									.append("INNER JOIN employer_user AS eu ON e.`id`=eu.`employer_id` WHERE eu.`primary_user`='Y' AND v.`id`="
											+ empId + "");
						}
					} else {
						queryString.append("FROM vw_subs v INNER JOIN employer AS e ON v.`subs_id`=e.`id` ");
						queryString
								.append("INNER JOIN employer_user AS eu ON e.`id`=eu.`employer_id` WHERE eu.`primary_user`='Y' AND v.`id`="
										+ empId + "");
					}
				}
			} else {
				if (CollectionUtils.isEmpty(subsIds)) {
					if (!isSubList) {
						List<Long> archievedIds = getArchievedSubsIds(empId);
						if (!CollectionUtils.isEmpty(archievedIds)) {
							queryString.append(
									"FROM project_sub_employer_mapping v INNER JOIN employer AS e ON v.`sub_id`=e.`id` ");
							queryString
									.append("INNER JOIN employer_user AS eu ON e.`id`=eu.`employer_id` WHERE eu.`primary_user`='Y' AND v.site_id="
											+ siteId + " AND v.`employer_id`=" + empId + " AND v.`sub_id` NOT IN("
											+ archievedIds.toString().replace("[", "").replace("]", "") + ")");
						} else {
							queryString.append(
									"FROM project_sub_employer_mapping v INNER JOIN employer AS e ON v.`sub_id`=e.`id` ");
							queryString
									.append("INNER JOIN employer_user AS eu ON e.`id`=eu.`employer_id` WHERE eu.`primary_user`='Y' AND v.site_id="
											+ siteId + " AND v.`employer_id`=" + empId + "");
						}
					} else {
						queryString.append(
								"FROM project_sub_employer_mapping v INNER JOIN employer AS e ON v.`sub_id`=e.`id` ");
						queryString
								.append("INNER JOIN employer_user AS eu ON e.`id`=eu.`employer_id` WHERE eu.`primary_user`='Y' AND v.site_id="
										+ siteId + " AND v.`employer_id`=" + empId + "");
					}

				} else {
					queryString
							.append("FROM `employer` AS e INNER JOIN `employer_user` AS eu ON e.`id`=eu.`employer_id` WHERE eu.`primary_user`='Y' AND e.`id` IN("
									+ subsIds.toString().replace("[", "").replace("]", "") + ")");
				}
			}

			if (siteId == 0 && null != isPriUser && isPriUser.equalsIgnoreCase("N")) {
				if (!StringUtils.isEmpty(searchChars)) {
					queryString2 = queryString2 + " WHERE (e.subName LIKE '%" + searchChars + "%' OR e.phoneNo LIKE '%"
							+ searchChars + "%')";
				}
				queryString2 = queryString2 + " GROUP BY e.id";
				Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString2);
				query.setResultTransformer(new AliasToBeanResultTransformer(SubForModuleDto.class));
				List<SubForModuleDto> subsList = query.list();
				int count = subsList.size();

				queryString2 = queryString2 + " ORDER BY subName ASC LIMIT 10 OFFSET " + offset;

				query = sessionFactory.getCurrentSession().createSQLQuery(queryString2);
				query.setResultTransformer(new AliasToBeanResultTransformer(SubForModuleDto.class));
				subsList = query.list();

				dataAndCountDto.setCount(count);
				dataAndCountDto.setSubsData(subsList);
			} else {
				if (!StringUtils.isEmpty(searchChars)) {
					queryString.append(" AND (e.employer_name LIKE '%" + searchChars + "%' OR eu.phone_number LIKE '%"
							+ searchChars + "%')");
				}
				queryString.append(" GROUP BY e.id");
				Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString.toString());
				query.setResultTransformer(new AliasToBeanResultTransformer(SubForModuleDto.class));
				List<SubForModuleDto> subsList = query.list();
				int count = subsList.size();

				if (!isSubList) {
					queryString.append(" ORDER BY subName ASC LIMIT 10 OFFSET " + offset);
				}

				query = sessionFactory.getCurrentSession().createSQLQuery(queryString.toString());
				query.setResultTransformer(new AliasToBeanResultTransformer(SubForModuleDto.class));
				subsList = query.list();

				dataAndCountDto.setCount(count);
				dataAndCountDto.setSubsData(subsList);
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.info("Exception in getSubsForSubsModule. " + e);
			for (int i = 0; i <= e.getStackTrace().length - 1; i++) {
				LOG.info(e.getStackTrace()[i].toString());
			}
		}
		return dataAndCountDto;
	}

	@Override
	public List<RuleEntity> getRuleListBySite(Long siteId) throws Exception {
		StringBuffer stringBuffer = new StringBuffer("SELECT * FROM rule WHERE site_id = " + siteId);
		Query query = sessionFactory.getCurrentSession().createSQLQuery(stringBuffer.toString())
				.addEntity(RuleEntity.class);
		return query.list();
	}

	@Override
	public List<NameIdDto> getDocNameIdById(Set<Long> documentIds) throws Exception {

		List<NameIdDto> docIdList = new ArrayList<NameIdDto>();
		try {
			if (!CollectionUtils.isEmpty(documentIds)) {
				String docIds = documentIds.toString().replace("[", "").replace("]", "");
				String queryString = "SELECT DISTINCT wdm.`doc_lib_id` AS id, dl.`user_document_name` AS name FROM `worker_doc_mapping` AS wdm\n"
						+ "INNER JOIN `document_library` AS dl ON wdm.`doc_lib_id`=dl.`id`\n" + "WHERE doc_lib_id IN("
						+ docIds + ") AND wdm.`status` NOT IN('Deleted') GROUP BY `doc_lib_id`;";
				Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
						.addScalar("id", StandardBasicTypes.LONG).addScalar("name", StandardBasicTypes.TEXT)
						.setResultTransformer(new AliasToBeanResultTransformer(NameIdDto.class));
				docIdList = query.list();
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.info("Exception in getDocNameIdById. " + e);
		}
		return docIdList;
	}

	@Override
	public List<NameIdDto> getVideoNameIdById(Set<Long> videoIds) throws Exception {

		List<NameIdDto> vidIdList = new ArrayList<NameIdDto>();
		try {
			if (!CollectionUtils.isEmpty(videoIds)) {
				String vidIds = videoIds.toString().replace("[", "").replace("]", "");
				String queryString = "SELECT DISTINCT id, `document_name` AS name FROM `document_library` WHERE id IN("
						+ vidIds + ") AND `employer_id`<>0;";
				Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
						.addScalar("id", StandardBasicTypes.LONG)
						.addScalar("name", StandardBasicTypes.TEXT)
						.setResultTransformer(new AliasToBeanResultTransformer(NameIdDto.class));
				vidIdList = query.list();
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.info("Exception in getVideoNameIdById. " + e);
		}
		return vidIdList;
	}

	@Override
	public WorkerDocMapping getWorkerDocumentMapping(Long docLibId, Long employerId, Long workerId) throws Exception {
		String queryString = "SELECT * FROM `worker_doc_mapping` WHERE `doc_lib_id`=" + docLibId + " AND `employer_id`="
				+ employerId + " AND `worker_id`=" + workerId
				+ " AND `status` NOT IN('Deleted') ORDER BY id DESC LIMIT 1";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(WorkerDocMapping.class);
		return (WorkerDocMapping) query.uniqueResult();
	}

	@Override
	public List<NameIdDto> getDocLibraryById(Set<Long> documentIds) throws Exception {
		String docIds = documentIds.toString().replace("[", "").replace("]", "");
		String queryString = "SELECT id, `user_document_name` AS name FROM `document_library` WHERE id IN(" + docIds
				+ ")";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
				.addScalar("id", StandardBasicTypes.LONG).addScalar("name", StandardBasicTypes.TEXT)
				.setResultTransformer(new AliasToBeanResultTransformer(NameIdDto.class));
		return query.list();
	}

	@Override
	public String getEmployerSkillSignalId(Long employerId) throws Exception {
		String queryString = "SELECT `skillsignal_id` FROM `employer` WHERE id=" + employerId;
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString);
		return (String) query.uniqueResult();
	}

	@Override
	public void updatePaidFlag(Long empId, String isPaid) throws Exception {

		String queryString = "UPDATE `employer` SET is_paid='" + isPaid + "' WHERE id=" + empId + ";";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString);
		query.executeUpdate();
	}

	@Override
	public List<DocLibMapping> getExistingDocLibMapping(Long docLibId, Long employerId, Long workerId)
			throws Exception {
		String queryString = "SELECT * FROM doc_lib_mapping WHERE `doc_lib_id`=" + docLibId + " AND `employer_id`="
				+ employerId + " AND `worker_id`=" + workerId + ";";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(DocLibMapping.class);
		return query.list();
	}

	@Override
	public ConfigurationEntity getConfigurationBySiteId(Long siteId) throws Exception {

		ConfigurationEntity configurationEntity = new ConfigurationEntity();
		Query query = null;
		try {
			final String queryString = "FROM ConfigurationEntity con WHERE con.siteId = :siteId";
			query = sessionFactory.getCurrentSession().createQuery(queryString).setParameter("siteId", siteId);
			configurationEntity = (ConfigurationEntity) query.uniqueResult();
		} catch (Exception e) {
			e.printStackTrace();
			LOG.info("Exception in getConfigurationBySiteId. " + e + ", siteId:" + siteId);
		}
		return configurationEntity;
	}

	@Override
	public Long getTodaysVisitorCount(Long siteId, String currDt) throws Exception {
		final String queryString = "SELECT COUNT(*) as visitorCount FROM `worker_survey` ws WHERE DATE(ws.updated_date)='"
				+ currDt + "' AND ws.visitor_id IS NOT NULL AND ws.site_id=" + siteId;

		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addScalar("visitorCount",
				StandardBasicTypes.LONG);
		return (Long) query.uniqueResult();
	}

	@Override
	public List<RuleEntity> getRuleListByDocId(Long docId, String docType) {
		String queryString = new String();
		if (StringUtils.equalsIgnoreCase(docType, "d")) {
			queryString = "SELECT * FROM rule WHERE FIND_IN_SET(" + docId + ",value_ids) > 0 AND actions = 'Fill'";
		} else if (StringUtils.equalsIgnoreCase(docType, "v")) {
			queryString = "SELECT * FROM rule WHERE FIND_IN_SET(" + docId + ",value_ids) > 0 AND actions = 'View'";
		}
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(RuleEntity.class);
		return query.list();
	}

	@Override
	public List<NameIdDto> getEmpUsersForConf(Long empId) throws Exception {
		final String queryString = "SELECT eu.id AS id, CONCAT(eu.first_name,' ',eu.last_name) AS `name`, eu.phone_number AS phNumber FROM employer_user eu WHERE primary_user<>'X' AND employer_id="
				+ empId;
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
				.addScalar("id", StandardBasicTypes.LONG).addScalar("name", StandardBasicTypes.TEXT)
				.addScalar("phNumber", StandardBasicTypes.LONG);
		return query.setResultTransformer(new AliasToBeanResultTransformer(NameIdDto.class)).list();
	}

	@Override
	public ConfigurationEntity getGlobalConfig(Long empUsrId) throws Exception {
		ConfigurationEntity configurationEntity = new ConfigurationEntity();
		Query query = null;
		try {
			final String queryString = "SELECT * FROM `configuration` WHERE emp_usr_id=" + empUsrId
					+ " AND `site_id` IS NULL";
			query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(ConfigurationEntity.class);
			configurationEntity = (ConfigurationEntity) query.uniqueResult();
		} catch (Exception e) {
			e.printStackTrace();
			LOG.info("Request >> empUsrId:" + empUsrId);
			LOG.info("Exception in getGlobalConfig. " + e);
		}
		return configurationEntity;
	}

	@Override
	public List<EmployerUser> getEmpUsersHavingAssignedSites(String zone) throws Exception {
		LOG.info("Start of getEmpUsersHavingAssignedSites >> zone : " + zone);
		/*
		 * final String queryString =
		 * "SELECT eu.* FROM employer_user eu INNER JOIN `project_key_person_entity` pke "
		 * +
		 * "ON eu.`id`=pke.`emp_user_id` INNER JOIN projects p ON pke.`site_id`=p.`id` "
		 * +
		 * "WHERE eu.`id`=pke.`emp_user_id` AND eu.`employer_id`<>p.`employer_id` AND p.`time_zone`='"
		 * + zone + "' GROUP BY eu.`id`";
		 */
		final String queryString = "SELECT DISTINCT a.*\n" + "FROM(SELECT DISTINCT eu.* \n"
				+ "	FROM employer_user eu \n"
				+ "	INNER JOIN `project_key_person_entity` pke ON eu.`id`=pke.`emp_user_id` \n"
				+ "	INNER JOIN projects p ON pke.`site_id`=p.`id` \n" + "	WHERE eu.`id`=pke.`emp_user_id` \n"
				+ "	AND p.`time_zone`='" + zone + "'\n" + "	UNION ALL \n" + "	SELECT DISTINCT eu.* \n"
				+ "	FROM employer_user eu \n"
				+ "	INNER JOIN `project_sub_employer_mapping` psem ON psem.sub_id=eu.employer_id\n"
				+ "	INNER JOIN `projects` p ON p.employer_id=eu.employer_id\n" + "	WHERE p.`time_zone`='" + zone
				+ "'\n" + "	UNION ALL\n" + "	SELECT DISTINCT eu.* \n" + "	FROM employer_user eu \n"
				+ "	INNER JOIN `projects` p ON p.`employer_id`=eu.`employer_id` \n"
				+ "	WHERE eu.`primary_user`='X' AND p.`time_zone`='" + zone + "') AS a\n" + "GROUP BY a.id";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(EmployerUser.class);
		return query.list();
	}

	@Override
	public List<EmployerUser> getEmpUsersHavingAssignedSites(String zone, Long siteId) throws Exception {

		String queryString = "";
		if (null == siteId)
			queryString = "SELECT DISTINCT a.*\n" + "FROM(SELECT DISTINCT eu.* \n" + "	FROM employer_user eu \n"
					+ "	INNER JOIN `project_key_person_entity` pke ON eu.`id`=pke.`emp_user_id` \n"
					+ "	INNER JOIN projects p ON pke.`site_id`=p.`id` \n" + "	WHERE eu.`id`=pke.`emp_user_id` \n"
					+ "	AND p.`time_zone`='" + zone + "'\n" + "	UNION ALL \n" + "	SELECT DISTINCT eu.* \n"
					+ "	FROM employer_user eu \n"
					+ "	INNER JOIN `project_sub_employer_mapping` psem ON psem.sub_id=eu.employer_id\n"
					+ "	INNER JOIN `projects` p ON p.employer_id=eu.employer_id\n" + "	WHERE p.`time_zone`='" + zone
					+ "'\n" + "	UNION ALL\n" + "	SELECT DISTINCT eu.* \n" + "	FROM employer_user eu \n"
					+ "	INNER JOIN `projects` p ON p.`employer_id`=eu.`employer_id` \n"
					+ "	WHERE eu.`primary_user`='X' AND p.`time_zone`='" + zone + "') AS a\n" + "GROUP BY a.id";
		else
			queryString = "SELECT DISTINCT a.*\n" + "FROM(SELECT DISTINCT eu.* \n" + "	FROM employer_user eu \n"
					+ "	INNER JOIN `project_key_person_entity` pke ON eu.`id`=pke.`emp_user_id` \n"
					+ "	INNER JOIN projects p ON pke.`site_id`=p.`id` \n" + "	WHERE eu.`id`=pke.`emp_user_id` \n"
					+ "	AND p.`id`='" + siteId + "'\n" + "	UNION ALL \n" + "	SELECT DISTINCT eu.* \n"
					+ "	FROM employer_user eu \n"
					+ "	INNER JOIN `project_sub_employer_mapping` psem ON psem.sub_id=eu.employer_id\n"
					+ "	INNER JOIN `projects` p ON p.employer_id=eu.employer_id\n" + "	WHERE p.`id`='" + siteId + "'\n"
					+ "	UNION ALL\n" + "	SELECT DISTINCT eu.* \n" + "	FROM employer_user eu \n"
					+ "	INNER JOIN `projects` p ON p.`employer_id`=eu.`employer_id` \n"
					+ "	WHERE eu.`primary_user`='X' AND p.`id`='" + siteId + "') AS a\n" + "GROUP BY a.id";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(EmployerUser.class);
		return query.list();
	}

	@Override
	public List<String> getNewWorkerGlobalContacts(Long employerId) throws Exception {
		String queryString = "SELECT c.new_worker_contacts FROM `configuration` AS c\n"
				+ "INNER JOIN `employer_user` AS eu ON eu.id=c.emp_usr_id\n" + "WHERE eu.employer_id=" + employerId
				+ " AND c.site_id IS NULL AND c.new_worker_global_flag IS TRUE;";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString);
		return query.list();
	}

	@Override
	public List<String> getDeniedWorkerGlobalContacts(Long employerId) throws Exception {
		String queryString = "SELECT c.`denied_worker_numbers` FROM `configuration` AS c\n"
				+ "INNER JOIN `employer_user` AS eu ON eu.id=c.emp_usr_id\n" + "WHERE eu.employer_id=" + employerId
				+ " AND c.site_id IS NULL AND c.`denied_worker_flag` IS TRUE;";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString);
		return query.list();
	}

	@Override
	public EmployerUser getGodEmployerUser(Long empId) throws Exception {
		final String queryString = "select usr from EmployerUser usr where usr.employerEntity.id=:empId and usr.primaryUser=:priUser";
		Query query = sessionFactory.getCurrentSession().createQuery(queryString).setParameter("empId", empId)
				.setParameter("priUser", "X");
		return (EmployerUser) query.uniqueResult();
	}

	@Override
	public DailyReportNotes saveDailyReportNotes(DailyReportNotes dailyReportNotes) throws Exception {
		return (DailyReportNotes) sessionFactory.getCurrentSession().merge(dailyReportNotes);
	}

	@Override
	public List<DailyReportNotes> getNotesByEmpId(Long employerId, String today, Long siteId) throws Exception {
		final String queryString = "SELECT * FROM daily_report_notes WHERE site_id=" + siteId
				+ " AND DATE(`time_stamp`)='" + today + "';";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(DailyReportNotes.class);
		return query.list();
	}

	@Override
	public List<WorkerDocMapping> isLibraryDocumentAssigned(Long docId) throws Exception {
		StringBuffer sb = new StringBuffer("FROM WorkerDocMapping WHERE docLibId = :docId AND status = :status");
		Query query = sessionFactory.getCurrentSession().createQuery(sb.toString()).setParameter("docId", docId)
				.setParameter("status", "Confirmed");
		return query.list();
	}

	@Override
	public List<WorkerDocListDto> getWorkerDocListForSSW(Long workerId) throws Exception {
		String queryString = "SELECT id AS docMapId, created_date AS createdDate, document_description AS docDesc, "
				+ "document_name AS docName, document_path AS docPath, user_document_name AS userDocName, "
				+ "'Uploaded' AS `status`, 'NA' AS siteName, 'NA' AS empName, employer_id AS `employerId`, doc_page_count AS docPageCount FROM document_library "
				+ "WHERE worker_id=" + workerId
				+ " UNION ALL SELECT wdm.id AS docMapId, dl.created_date AS createdDate, "
				+ "dl.document_description AS docDesc, dl.document_name AS docName, wdm.document_path AS docPath, "
				+ "dl.user_document_name AS userDocName, wdm.`status`, p.name AS siteName, e.employer_name AS empName, p.employer_id AS employerId, dl.doc_page_count AS docPageCount "
				+ "FROM document_library dl INNER JOIN worker_doc_mapping wdm ON dl.id=wdm.doc_lib_id "
				+ "INNER JOIN projects p ON wdm.site_id=p.id INNER JOIN employer e ON p.employer_id=e.id "
				+ "WHERE wdm.worker_id=" + workerId + " AND wdm.status NOT IN('Deleted');";

		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
				.setResultTransformer(new AliasToBeanResultTransformer(WorkerDocListDto.class));
		return query.list();
	}

	@Override
	public List<WorkerDocListDto> getWorkerDocListForSSID(Long workerId, Long employerId, Long siteId)
			throws Exception {
		String queryString = "SELECT wdm.id AS docMapId, dl.created_date AS createdDate, "
				+ "dl.document_description AS docDesc, dl.document_name AS docName, wdm.document_path AS docPath, "
				+ "dl.user_document_name AS userDocName, wdm.`status`, p.name AS siteName, e.employer_name AS empName, p.employer_id AS employerId, dl.doc_page_count AS docPageCount "
				+ "FROM document_library dl INNER JOIN worker_doc_mapping wdm ON dl.id=wdm.doc_lib_id "
				+ "INNER JOIN projects p ON wdm.site_id=p.id INNER JOIN employer e ON p.employer_id=e.id "
				+ "WHERE wdm.worker_id=" + workerId + " AND wdm.status NOT IN('Deleted')  "
				+ " AND wdm.status IN ('Assigned', 'Confirmed') AND  wdm.site_id=" + siteId;

		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
				.setResultTransformer(new AliasToBeanResultTransformer(WorkerDocListDto.class));
		return query.list();
	}

	@Override
	public WorkerDocMapping addWorkerDocMapping(WorkerDocMapping workerDocMapping) {
		return (WorkerDocMapping) sessionFactory.getCurrentSession().merge(workerDocMapping);
	}

	@Override
	public List<WorkerDocMapping> checkWorkerDocMappingExists(Long docId, Long workerId, Long siteId) {
		final String queryString = "SELECT * FROM `worker_doc_mapping` WHERE `doc_lib_id`=" + docId
				+ " AND `worker_id`=" + workerId + " AND `site_id`=" + siteId + ";";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(WorkerDocMapping.class);
		return query.list();
	}

	@Override
	public RuleEntity getRuleById(Long ruleId) throws Exception {
		Query query = sessionFactory.getCurrentSession().createQuery("FROM RuleEntity where id = :ruleId")
				.setParameter("ruleId", ruleId);
		return (RuleEntity) query.uniqueResult();
	}

	@Override
	public void deleteMappingForRule(Long siteId, Set<Long> documentIds, String action) throws Exception {
		String queryString = new String();
		if (action.equals("Fill")) {
			queryString = "DELETE FROM WorkerDocMapping wdm WHERE wdm.siteId = " + siteId
					+ " AND wdm.status IN ('Assigned') AND wdm.docLibId IN (:docId)";
		} else if (action.equals("View")) {
			queryString = "DELETE FROM DocLibMapping WHERE siteId=" + siteId + " AND docLibId IN(:docId)";
		}
		Query query = sessionFactory.getCurrentSession().createQuery(queryString).setParameterList("docId",
				documentIds);
		query.executeUpdate();
	}

	@Override
	public List<WorkerDocMapping> getSubmittedWorkerList(Long siteId, Set<Long> docIdSet) throws Exception {
		StringBuffer sb = new StringBuffer("FROM WorkerDocMapping wdm WHERE wdm.siteId = " + siteId
				+ " AND wdm.status = 'Confirmed' AND wdm.docLibId IN (:docId) AND wdm.status NOT IN('Deleted')");
		Query query = sessionFactory.getCurrentSession().createQuery(sb.toString()).setParameterList("docId", docIdSet);
		return query.list();
	}

	@Override
	public void deleteAssignedDocumentByWorkerId(Long siteId, Set<Long> docIdSet, Set<Long> submittedWorkerIdList)
			throws Exception {
		StringBuffer sb = new StringBuffer("DELETE WorkerDocMapping wdm WHERE wdm.siteId = " + siteId).append(
				" AND wdm.status = 'Assigned' AND wdm.docLibId IN (:docId) AND wdm.workerId IN (:workerIdList)");
		Query query = sessionFactory.getCurrentSession().createQuery(sb.toString()).setParameterList("docId", docIdSet)
				.setParameterList("workerIdList", submittedWorkerIdList);
		query.executeUpdate();
	}

	@Override
	public List<Long> getWorkerDocLibMapping(Long docId, Long siteEmpId, Long workerId, Long siteId) {

		List<Long> workerDocMappingIds = new ArrayList<Long>();
		Query query = null;
		try {
			final String queryString = "SELECT id FROM `worker_doc_mapping` WHERE `doc_lib_id`=" + docId
					+ " AND `employer_id`=" + siteEmpId + " AND `worker_id`=" + workerId + " AND `site_id`=" + siteId
					+ ";";
			query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addScalar("id",
					StandardBasicTypes.LONG);
			workerDocMappingIds = query.list();
		} catch (Exception e) {
			e.printStackTrace();
			LOG.info("Exception in getWorkerDocLibMapping. " + e);
		}
		return workerDocMappingIds;
	}

	@Override
	public Long getSiteIdOfRule(Long ruleId) {

		Query query = null;
		try {
			final String queryString = "SELECT `site_id` FROM `rule` WHERE id=" + ruleId + ";";
			query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addScalar("site_id",
					StandardBasicTypes.LONG);
		} catch (Exception e) {
			e.printStackTrace();
			LOG.info("Exception in getSiteIdOfRule. " + e);
		}
		return (Long) query.uniqueResult();
	}

	@Override
	public WorkerDocMapping getSiteSpecificWorkerDocMapping(Long docLibId, Long employerId, Long workerId,
			Long siteId) {
		WorkerDocMapping workerDocMapping = new WorkerDocMapping();
		try {
			final String queryString = "SELECT * FROM `worker_doc_mapping` WHERE doc_lib_id=" + docLibId
					+ " AND `employer_id`=" + employerId + " AND `worker_id`=" + workerId + " AND `site_id`=" + siteId
					+ " AND status NOT IN('Deleted');";
			Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
					.addEntity(WorkerDocMapping.class);
			workerDocMapping = (WorkerDocMapping) query.uniqueResult();
		} catch (Exception e) {
			e.printStackTrace();
			LOG.info("Exception in getSiteSpecificWorkerDocMapping. " + e);
		}
		return workerDocMapping;
	}

	@Override
	public List<WorkerDocMapping> getWorkerSiteDocs(Long employerId, Long workerId, Long siteId) {
		List<WorkerDocMapping> workerDocMappings = new ArrayList<WorkerDocMapping>();
		try {
			final String queryString = "SELECT * FROM `worker_doc_mapping` WHERE `employer_id`=" + employerId
					+ " AND `worker_id`=" + workerId + " AND `site_id`=" + siteId + " AND `status` NOT IN('Deleted');";
			Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
					.addEntity(WorkerDocMapping.class);
			workerDocMappings = query.list();
		} catch (Exception e) {
			e.printStackTrace();
			LOG.info("Exception in getWorkerSiteDocs. " + e);
		}
		return workerDocMappings;
	}

	@Override
	public List<WorkerDocMapping> getWorkerDocMapping(Long employerId, Long workerId) {
		List<WorkerDocMapping> workerDocMappings = new ArrayList<WorkerDocMapping>();
		try {
			final String queryString = "SELECT * FROM `worker_doc_mapping` WHERE `employer_id`=" + employerId
					+ " AND `worker_id`=" + workerId + " AND status NOT IN('Deleted');";
			Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
					.addEntity(WorkerDocMapping.class);
			workerDocMappings = query.list();
		} catch (Exception e) {
			e.printStackTrace();
			LOG.info("Exception in getWorkerDocMapping. " + e);
		}
		return workerDocMappings;
	}

	@Override
	public void deleteAssignedWorkerDocs(Long siteId, Long employerId, Set<Long> existingSiteWorkers) {
		try {
			if (!CollectionUtils.isEmpty(existingSiteWorkers)) {
				final String queryString = "DELETE FROM `worker_doc_mapping` WHERE `employer_id`=" + employerId
						+ " AND `site_id`=" + siteId + " AND `worker_id`IN("
						+ existingSiteWorkers.toString().replace("[", "").replace("]", "")
						+ ") AND `status` IN('Assigned','Drafted');";
				Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString);
				query.executeUpdate();
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.info("Exception in deleteAssignedWorkerDocs. " + e);
		}
	}

	@Override
	public void deleteAssignedDraftedDocs(Long docId) {
		try {
			// delete from mapping
			final String queryString2 = "DELETE FROM `worker_doc_mapping` WHERE doc_lib_id=" + docId
					+ " AND `status`IN('Assigned');";
			Query query2 = sessionFactory.getCurrentSession().createSQLQuery(queryString2);
			query2.executeUpdate();
		} catch (Exception e) {
			e.printStackTrace();
			LOG.info("Exception in deleteAssignedDarftedDocs. " + e);
		}
	}

	public void addOrUpdateRule(RuleEntity ruleEntity) {
		sessionFactory.getCurrentSession().merge(ruleEntity);
	}

	@Override
	public List<WorkerDocMapping> getDocMappingByWorkerId(Long workerId) {
		final String queryString = "SELECT * FROM `worker_doc_mapping` WHERE `worker_id`=" + workerId
				+ " AND `status` IN('Drafted');";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(WorkerDocMapping.class);
		return query.list();
	}

	@Override
	public void deleteDocMappingByWorkerId(Long workerId) {
		final String queryString = "DELETE FROM `worker_doc_mapping` WHERE `worker_id`=" + workerId + " ;";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString);
		query.executeUpdate();
	}

	@Override
	public WorkerDocMapping getDocMappingByWorkerIdSiteId(Long workerId, Long siteId, Long docId) {
		final String queryString = "SELECT * FROM `worker_doc_mapping` WHERE `worker_id`=" + workerId
				+ " AND `site_id`=" + siteId + " AND `doc_lib_id`=" + docId + "; AND status NOT IN('Deleted')";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(WorkerDocMapping.class);
		return (WorkerDocMapping) query.uniqueResult();
	}

	@Override
	public List<WorkerDocMapping> getAssignDocsByWorkerSite(Long workerId, Long siteId) {
		final String queryString = "SELECT * FROM `worker_doc_mapping` WHERE `worker_id`=" + workerId
				+ " AND `site_id`=" + siteId + " AND `status` IN('Drafted');";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(WorkerDocMapping.class);
		return query.list();
	}

	@Override
	//before adding isDailyReport filter.Added isDailyReport because we need to include archieved projects in daily report
	//public List<NameIdDto> getOwnAssignedProjects(Long empId) {
	//after
	public List<NameIdDto> getOwnAssignedProjects(Long empId,boolean isDailyReport,EmployerUser empUser) {
		// GPS tracking is added for Location mismatch
		//before adding isDailyReport filter
		//final String queryString = "SELECT DISTINCT a.id, a.name, a.siteCode, a.employerId, a.createdDate,  a.gpsTrackingFlag  \n"
		//after
		String queryString = "SELECT DISTINCT a.id, a.name, a.siteCode, a.employerId, a.createdDate,  a.gpsTrackingFlag  \n"
				+ "FROM(SELECT p.id, p.`name`, p.site_code AS siteCode, p.employer_id AS employerId, p.created_date AS createdDate ,  c.gps_tracking_flag AS gpsTrackingFlag "
				+ " FROM projects p " + "  INNER JOIN configuration c  ON c.id=p.configuration_id  "
				+ " WHERE employer_id=" + empId + "  AND p.`is_active`='Y' \n" + "	UNION ALL\n"
				+ "	SELECT p.id,p.`name`,p.site_code AS siteCode,p.employer_id AS employerId, p.created_date AS createdDate , c.gps_tracking_flag AS gpsTrackingFlag "
				+ "FROM `projects` p\n" + "  INNER JOIN configuration c  ON c.id=p.configuration_id  "
				+ "	INNER JOIN `project_sub_employer_mapping` psem ON p.`id` = psem.`site_id` \n"
				+ "	WHERE  psem.`sub_id`=" + empId + "  AND p.`is_active`='Y' \n" + "	UNION ALL\n"
				+ "	SELECT p.id,p.`name`,p.site_code AS siteCode,p.employer_id AS employerId, p.created_date AS createdDate , c.gps_tracking_flag AS gpsTrackingFlag "
				+ "FROM `projects` p \n" + "  INNER JOIN configuration c  ON c.id=p.configuration_id  "
				+ "	INNER JOIN `project_key_person_entity` pke ON p.`id`=pke.`site_id` \n"
				+ "	INNER JOIN `employer_user` eu ON eu.id=pke.`emp_user_id` \n" + " WHERE eu.employer_id=" + empId
				+ " AND `ssid_access` IS TRUE  AND p.`is_active`='Y') AS a ";
				// added to remove archieved site from primary ,kios and
				// secondary user in SSID
				//add archieved sites filter if not daily report
				if(!isDailyReport)
					queryString=queryString+ " WHERE a.id NOT IN (SELECT DISTINCT site_id FROM project_archive where employer_user_id ="+empUser.getId() +" and employer_id="+empUser.getEmployerEntity().getId()+" ) ";
				
				queryString=queryString+ " \n " + "ORDER BY a.name ASC";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
				.addScalar("id", StandardBasicTypes.LONG).addScalar("name", StandardBasicTypes.TEXT)
				.addScalar("siteCode", StandardBasicTypes.TEXT).addScalar("employerId", StandardBasicTypes.LONG)
				.addScalar("createdDate", StandardBasicTypes.TEXT)
				.addScalar("gpsTrackingFlag", StandardBasicTypes.BOOLEAN)
				.setResultTransformer(new AliasToBeanResultTransformer(NameIdDto.class));

		return query.list();
	}

	@Override
	//before get  project removing archiving  project when Sebastein reported issue on prod
	//public List<NameIdDto> getOwnAssignedProjectsForBothActiveAndInactive(Long empId) {
	//after get  project removing archiving  project when Sebastein reported issue on prod
	public List<NameIdDto> getOwnAssignedProjectsForBothActiveAndInactive(Long empId,Long empUserId) {
		// GPS tracking is added for Location mismatch
		/*final String queryString = "SELECT DISTINCT a.id, a.name, a.siteCode, a.employerId, a.createdDate,  a.gpsTrackingFlag  \n"
				+ "FROM(SELECT p.id, p.`name`, p.site_code AS siteCode, p.employer_id AS employerId, p.created_date AS createdDate ,  c.gps_tracking_flag AS gpsTrackingFlag "
				+ " FROM projects p " + "  INNER JOIN configuration c  ON c.id=p.configuration_id  "
				+ " WHERE employer_id=" + empId + "  \n" + "	UNION ALL\n"
				+ "	SELECT p.id,p.`name`,p.site_code AS siteCode,p.employer_id AS employerId, p.created_date AS createdDate , c.gps_tracking_flag AS gpsTrackingFlag "
				+ "FROM `projects` p\n" + "  INNER JOIN configuration c  ON c.id=p.configuration_id  "
				+ "	INNER JOIN `project_sub_employer_mapping` psem ON p.`id` = psem.`site_id` \n"
				+ "	WHERE  psem.`sub_id`=" + empId + "  \n" + "	UNION ALL\n"
				+ "	SELECT p.id,p.`name`,p.site_code AS siteCode,p.employer_id AS employerId, p.created_date AS createdDate , c.gps_tracking_flag AS gpsTrackingFlag "
				+ "FROM `projects` p \n" + "  INNER JOIN configuration c  ON c.id=p.configuration_id  "
				+ "	INNER JOIN `project_key_person_entity` pke ON p.`id`=pke.`site_id` \n"
				+ "	INNER JOIN `employer_user` eu ON eu.id=pke.`emp_user_id` \n" + " WHERE eu.employer_id=" + empId
				+ " AND `ssid_access` IS TRUE  ) AS a\n" + "ORDER BY a.name ASC";*/
		//get  project removing archiving  project when Sebastein reported issue on prod
		final String queryString = "SELECT DISTINCT a.id, a.name, a.siteCode, a.employerId, a.createdDate,  a.gpsTrackingFlag  \n"
				+ "FROM(SELECT p.id, p.`name`, p.site_code AS siteCode, p.employer_id AS employerId, p.created_date AS createdDate ,  c.gps_tracking_flag AS gpsTrackingFlag "
				+ " FROM projects p " + "  INNER JOIN configuration c  ON c.id=p.configuration_id  "
				+ " WHERE employer_id=" + empId + "  \n" + "	AND p.id NOT IN (select DISTINCT site_id from project_archive where employer_id="+empId+" AND employer_user_id ="+empUserId+" ) UNION ALL\n"
				+ "	SELECT p.id,p.`name`,p.site_code AS siteCode,p.employer_id AS employerId, p.created_date AS createdDate , c.gps_tracking_flag AS gpsTrackingFlag "
				+ "FROM `projects` p\n" + "  INNER JOIN configuration c  ON c.id=p.configuration_id  "
				+ "	INNER JOIN `project_sub_employer_mapping` psem ON p.`id` = psem.`site_id` \n"
				+ "	WHERE  psem.`sub_id`=" + empId + "  AND p.id NOT IN (select DISTINCT site_id from project_archive where employer_id="+empId+" AND employer_user_id ="+empUserId+" ) \n" + "	UNION ALL\n"
				+ "	SELECT p.id,p.`name`,p.site_code AS siteCode,p.employer_id AS employerId, p.created_date AS createdDate , c.gps_tracking_flag AS gpsTrackingFlag "
				+ "FROM `projects` p \n" + "  INNER JOIN configuration c  ON c.id=p.configuration_id  "
				+ "	INNER JOIN `project_key_person_entity` pke ON p.`id`=pke.`site_id` \n"
				+ "	INNER JOIN `employer_user` eu ON eu.id=pke.`emp_user_id` \n" + " WHERE eu.employer_id=" + empId
				+ " AND `ssid_access` IS TRUE AND p.id NOT IN (select DISTINCT site_id from project_archive where employer_id="+empId+" AND employer_user_id ="+empUserId+" )   ) AS a\n" + "ORDER BY a.name ASC";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
				.addScalar("id", StandardBasicTypes.LONG).addScalar("name", StandardBasicTypes.TEXT)
				.addScalar("siteCode", StandardBasicTypes.TEXT).addScalar("employerId", StandardBasicTypes.LONG)
				.addScalar("createdDate", StandardBasicTypes.TEXT)
				.addScalar("gpsTrackingFlag", StandardBasicTypes.BOOLEAN)
				.setResultTransformer(new AliasToBeanResultTransformer(NameIdDto.class));

		return query.list();
	}

	@Override
	public void updateVideoDocStatus(String docType, Long workerId, Long siteId, List<Long> docIds, String date) {
		String queryString = new String();
		if (StringUtils.equals(docType, "d")) {
			queryString = "UPDATE `worker_doc_mapping` SET `status`='Deleted', deleted_dt='" + date
					+ "' WHERE `worker_id`=" + workerId + " AND `site_id`=" + siteId + " AND `doc_lib_id` IN(:docIds);";
		} else if (StringUtils.equals(docType, "v")) {
			queryString = "UPDATE `doc_lib_mapping` SET `is_seen`='t', seen_date='" + date + "' WHERE `worker_id`="
					+ workerId + " AND `doc_lib_id` IN(:docIds);";
		}
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).setParameterList("docIds", docIds);
		query.executeUpdate();
	}

	@Override
	public List<DocLibMapping> checkWorkerVidMappingExists(Long docId, Long workerId) {
		final String queryString = "SELECT * FROM doc_lib_mapping WHERE doc_lib_id=" + docId + " AND  worker_id="
				+ workerId + ";";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString);
		return query.list();
	}

	@Override
	public void deleteVideoMappingForRule(Long siteId, Set<Long> delDocId) {
		StringBuffer sb = new StringBuffer(
				"DELETE FROM `doc_lib_mapping` WHERE `site_id`=" + siteId + " AND `doc_lib_id`IN(:delDocId);");
		Query query = sessionFactory.getCurrentSession().createQuery(sb.toString()).setParameterList("delDocId",
				delDocId);
		query.executeUpdate();
	}

	@Override
	public List<WorkerDocMapping> getDeletedWorkerSiteDocs(Long employerId, Long workerId, Long siteId) {
		List<WorkerDocMapping> workerDocMappings = new ArrayList<WorkerDocMapping>();
		try {
			final String queryString = "SELECT * FROM `worker_doc_mapping` WHERE `employer_id`=" + employerId
					+ " AND `worker_id`=" + workerId + " AND `site_id`=" + siteId + " AND `status` IN('Deleted');";
			Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
					.addEntity(WorkerDocMapping.class);
			workerDocMappings = query.list();
		} catch (Exception e) {
			e.printStackTrace();
			LOG.info("Exception in getWorkerSiteDocs. " + e);
		}
		return workerDocMappings;
	}

	@Override
	public Long getEmpUserIdByEmpId(Long employerId) {
		LOG.info("Start of getEmpUserIdByEmpId. " + employerId);
		final String queryString = "SELECT id FROM `employer_user` WHERE `employer_id`=" + employerId
				+ " AND `primary_user`='Y' LIMIT 1;";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addScalar("id",
				StandardBasicTypes.LONG);
		return (Long) query.uniqueResult();
	}

	@Override
	public DocLibMapping getDocLibMappingByWorkerIdSiteId(Long id, Long workerId, Long siteId) {
		final String queryString = "SELECT * FROM `doc_lib_mapping` WHERE `worker_id`=" + workerId
				+ " AND `doc_lib_id`=" + id + " AND `site_id`=" + siteId + ";";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(DocLibMapping.class);
		return (DocLibMapping) query.uniqueResult();
	}

	@Override
	public List<LibraryListForApp> getLibraryListForAppBySite(Long employerId, Long workerId, String doctype,
			Long siteId) {
		List<LibraryListForApp> libraryListForApps = new ArrayList<LibraryListForApp>();

		final String queryString = "SELECT DISTINCT dl.id, dl.`added_by`, dl.`document_description`, dl.`document_name`, dl.`document_path`, dlm.`is_seen`, dlm.`is_ack`, dl.thumbnail_path, dlm.`seen_date` FROM document_library dl INNER JOIN `doc_lib_mapping` dlm ON dl.id=dlm.`doc_lib_id` WHERE dlm.`worker_id`="
				+ workerId + " AND dlm.site_id=" + siteId + " AND dl.`doc_type`='v' AND dl.`is_active`=TRUE";

		try (Connection con = (Connection) ConnectionFactory.getConnection("db1");
				PreparedStatement ps = (PreparedStatement) con.prepareStatement(queryString);) {
			try (ResultSet rs = ps.executeQuery();) {

				while (rs.next()) {

					LibraryListForApp libraryListForApp = new LibraryListForApp();
					libraryListForApp.setId(rs.getLong("id"));
					libraryListForApp.setDocTitle(rs.getString("document_name"));
					libraryListForApp.setDocDesc(rs.getString("document_description"));
					libraryListForApp.setDocLink(rs.getString("document_path"));
					libraryListForApp.setPublishedBy(rs.getString("added_by"));
					libraryListForApp.setIsAck(rs.getString("is_ack"));
					libraryListForApp.setIsSeen(rs.getString("is_seen"));
					libraryListForApp.setThumbnailPath(rs.getString("thumbnail_path"));
					libraryListForApp.setSeenDate(rs.getString("seen_date"));

					libraryListForApps.add(libraryListForApp);
				}
			} catch (Exception e) {
				LOG.info("Exception occured while getFromLibraryForApp : " + e);
			}
		} catch (Exception e) {
			LOG.info("Exception occured while getFromLibraryForApp : " + e);
		}

		return libraryListForApps;
	}

	@Override
	public List<Long> getEmpUsersByEmpId(Long employerId) {
		final String queryString = "SELECT id FROM `employer_user` WHERE `employer_id`=" + employerId
				+ " AND (`is_kiosk_user`='N' OR `is_kiosk_user` IS NULL);";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addScalar("id",
				StandardBasicTypes.LONG);
		return query.list();
	}

	@Override
	public List<RuleEntity> getContainedDelVidRules(Long siteId, Long vidId) {
		StringBuffer sb = new StringBuffer(
				"SELECT * FROM `rule` WHERE FIND_IN_SET(" + vidId + ", `value_ids`) AND `actions`='Fill'");
		Query query = sessionFactory.getCurrentSession().createSQLQuery(sb.toString());
		return query.list();
	}

	@Override
	public void deleteOwnVidLibMapping(Long delVidId, Long siteId) {
		final String queryString = "DELETE dlm.* \n"
				+ "FROM `doc_lib_mapping` dlm INNER JOIN `project_workers_mapping` pwm ON dlm.worker_id=pwm.worker_id \n"
				+ "WHERE dlm.`doc_lib_id`=" + delVidId + " AND pwm.`site_id`=" + siteId + " AND `subs_id` IS NULL;";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString);
		query.executeUpdate();
	}

	@Override
	public void deleteAllWorkerVidLibMapping(Long delVidId, Long siteId) {
		final String queryString = "DELETE dlm.* FROM `doc_lib_mapping` AS dlm INNER JOIN `project_workers_mapping` AS pwm ON pwm.`worker_id`=dlm.worker_id WHERE dlm.doc_lib_id="
				+ delVidId + " AND pwm.`site_id`=" + siteId + ";";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString);
		query.executeUpdate();
	}

	@Override
	public void deleteSubVidLibMapping(Long delVidId, Long siteId, Set<Long> subId) {
		final String queryString = "DELETE dlm.* FROM `doc_lib_mapping` AS dlm INNER JOIN `project_workers_mapping` AS pwm ON pwm.`worker_id`=dlm.`worker_id` WHERE dlm.`doc_lib_id`="
				+ delVidId + " AND pwm.`site_id`=" + siteId + " AND pwm.`subs_id`IN (:subId)";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).setParameterList("subId", subId);
		query.executeUpdate();
	}

	@Override
	public List<Long> getVidMappedWorkers(Long docId) {
		final String queryString = "SELECT DISTINCT `worker_id` FROM `doc_lib_mapping` WHERE `doc_lib_id`=" + docId
				+ ";";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addScalar("worker_id",
				StandardBasicTypes.LONG);
		return query.list();
	}

	@Override
	public List<NameIdDto> getWatchedVidsByWorkerId(Long workerId, Long employerId, List<Long> siteId) {

		List<NameIdDto> nameId = new ArrayList<>();
		Query query1 = null;
		if (null != siteId && !siteId.equals(new Long(0))) {

			final String queryString1 = "select profile from SkillSignalProfileEntity profile where profile.id =:id";
			Query query = sessionFactory.getCurrentSession().createQuery(queryString1).setParameter("id", workerId);
			SkillSignalProfileEntity skillSignalProfileEntity = (SkillSignalProfileEntity) query.uniqueResult();
			HashSet<Long> uniquelistOfRuleId = new HashSet<>();

			if (skillSignalProfileEntity.getEmployerEntity().getId().equals(employerId)) {

				final String ruleQuery = "SELECT * FROM rule r WHERE r.`actions`='View' AND r.`site_id` IN (:siteId)   AND (r.user_type='O' OR r.user_type='W' OR r.user_type='O,S')";

				Query rule = ((SQLQuery) sessionFactory.getCurrentSession().createSQLQuery(ruleQuery)
						.setParameterList("siteId", siteId)).addEntity(RuleEntity.class);
				List<RuleEntity> listOfRule = rule.list();
				if (!CollectionUtils.isEmpty(listOfRule)) {
					for (RuleEntity ruleEntity : listOfRule) {
						if (StringUtils.isNotBlank(ruleEntity.getValueIds())) {
							String[] ruleId = ruleEntity.getValueIds().split(",");

							for (String ruleIdValue : ruleId) {
								uniquelistOfRuleId.add(Long.valueOf(ruleIdValue.trim()));
							}
						}
					}
					if (uniquelistOfRuleId.size() > 0) {
						// for(Long ruleId : uniquelistOfRuleId)
						// {
						final String queryString = "SELECT dlm.doc_lib_id AS `id` , dl.document_name AS `name`\n"
								+ "FROM `doc_lib_mapping` AS dlm INNER JOIN `document_library` AS dl ON dlm.doc_lib_id=dl.id \n"
								+ "WHERE dlm.`worker_id`=" + workerId
								+ " AND dlm.`is_seen`='t' AND dlm.employer_id<>0   AND  dlm.doc_lib_id IN(:ruleId);";
						query1 = sessionFactory.getCurrentSession().createSQLQuery(queryString)
								.addScalar("id", StandardBasicTypes.LONG).addScalar("name", StandardBasicTypes.TEXT)
								.setParameterList("ruleId", uniquelistOfRuleId)
								.setResultTransformer(new AliasToBeanResultTransformer(NameIdDto.class));

						nameId = query1.list();

						// List<NameIdDto> nameId1= query1.list();
						// if(!CollectionUtils.isEmpty(nameId1))
						// {
						// nameId.addAll(nameId1);
						// }
						// }
					}
				}
			} else {
				String ruleQuery = null;
				if (null != skillSignalProfileEntity.getEmployerEntity()) {
					ruleQuery = "SELECT * FROM rule r WHERE r.`actions`='View' AND r.`site_id` IN(:siteId) AND (r.user_type='S' OR r.user_type='W' OR r.user_type='O,S')   AND (  FIND_IN_SET("
							+ skillSignalProfileEntity.getEmployerEntity().getId()
							+ ",r.subs_ids )   OR r.subs_ids IS  NULL)";
				} else {
					ruleQuery = "SELECT * FROM rule r WHERE r.`actions`='View' AND r.`site_id` IN(:siteId) AND (r.user_type='S' OR r.user_type='W' OR r.user_type='O,S')   AND  r.subs_ids IS  NULL";
				}
				Query rule = ((SQLQuery) sessionFactory.getCurrentSession().createSQLQuery(ruleQuery)
						.setParameterList("siteId", siteId)).addEntity(RuleEntity.class);
				List<RuleEntity> listOfRule = rule.list();
				if (!CollectionUtils.isEmpty(listOfRule)) {
					for (RuleEntity ruleEntity : listOfRule) {
						if (StringUtils.isNotBlank(ruleEntity.getValueIds())) {
							String[] ruleId = ruleEntity.getValueIds().split(",");

							for (String ruleIdValue : ruleId) {
								uniquelistOfRuleId.add(Long.valueOf(ruleIdValue.trim()));
							}
						}
					}
					if (uniquelistOfRuleId.size() > 0) {
						for (Long ruleId : uniquelistOfRuleId) {
							final String queryString = "SELECT dlm.doc_lib_id AS `id` , dl.document_name AS `name`\n"
									+ "FROM `doc_lib_mapping` AS dlm INNER JOIN `document_library` AS dl ON dlm.doc_lib_id=dl.id \n"
									+ "WHERE dlm.`worker_id`=" + workerId
									+ " AND dlm.`is_seen`='t' AND dlm.employer_id<>0   AND  dlm.doc_lib_id IN(" + ruleId
									+ ");";
							query1 = sessionFactory.getCurrentSession().createSQLQuery(queryString)
									.addScalar("id", StandardBasicTypes.LONG).addScalar("name", StandardBasicTypes.TEXT)
									.setResultTransformer(new AliasToBeanResultTransformer(NameIdDto.class));
							List<NameIdDto> nameId1 = query1.list();
							if (!CollectionUtils.isEmpty(nameId1)) {
								nameId.addAll(nameId1);
							}
						}
					}
				}
			}
		}
		// else
		// {

		/*
		 * final String queryString =
		 * "SELECT dlm.doc_lib_id AS `id` , dl.document_name AS `name`\n" +
		 * "FROM `doc_lib_mapping` AS dlm INNER JOIN `document_library` AS dl ON dlm.doc_lib_id=dl.id \n"
		 * + "WHERE dlm.`worker_id`=" + workerId +
		 * " AND dlm.`is_seen`='t' AND dlm.employer_id<>0;"; Query query =
		 * sessionFactory.getCurrentSession().createSQLQuery(queryString)
		 * .addScalar("id", StandardBasicTypes.LONG).addScalar("name",
		 * StandardBasicTypes.TEXT) .setResultTransformer(new
		 * AliasToBeanResultTransformer(NameIdDto.class)); nameId= query.list();
		 */

		// }
		return nameId;
	}

	@Override
	public List<EmployerEntity> getEmpHavingClkInWorkers(String strDate) {
		List<EmployerEntity> employerList = new ArrayList<EmployerEntity>();
		try {
			String date = DateFormatConstants.FORMAT5.format(DateFormatConstants.FORMAT8.parse(strDate));
			final String queryString = "SELECT DISTINCT e.*\n"
					+ "FROM `employer` AS e INNER JOIN `skillsignal_profile` AS sp ON sp.`employer_id`=e.`id` INNER JOIN `attendance` AS a ON a.`worker_id`=sp.`id`\n"
					+ "WHERE DATE(a.`clock_in_time`)='" + date + "' ORDER BY e.`id`;";
			Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
					.addEntity(EmployerEntity.class);
			employerList = query.list();
		} catch (ParseException e) {
			e.printStackTrace();
			LOG.info("Exception in getEmpHavingClkInWorkers. " + e);
		}
		return employerList;
	}

	@Override
	public List<WorkerDocMapping> getWorkersConfirmedDocs(Long workerId) {
		final String queryString = "SELECT * FROM `worker_doc_mapping` WHERE `worker_id`=" + workerId
				+ " AND `status`IN('Confirmed');";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(WorkerDocMapping.class);
		return query.list();
	}

	@Override
	public ArchievedSubsEntity addOrUpdateArchievedSubsEntity(ArchievedSubsEntity archievedSubEntity) {
		return (ArchievedSubsEntity) sessionFactory.getCurrentSession().merge(archievedSubEntity);
	}

	@Override
	public List<Long> getArchievedSubsIds(Long mainEmployerId) {
		final String queryString = "SELECT DISTINCT `sub_employer_id` FROM `archieved_subs` WHERE `main_employer_id`="
				+ mainEmployerId + "";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addScalar("sub_employer_id",
				StandardBasicTypes.LONG);
		return query.list();
	}

	@Override
	public int activateArchievedSub(Long mainEmployerId, Long subEmployerId) {
		LOG.info("activateArchievedSub >> ");
		int result = 0;
		final String queryString = "DELETE FROM ArchievedSubsEntity mas WHERE mas.mainEmployerId=:mainEmployerId and mas.subEmployerId=:subEmployerId";
		Query query = sessionFactory.getCurrentSession().createQuery(queryString);
		query.setParameter("mainEmployerId", mainEmployerId);
		query.setParameter("subEmployerId", subEmployerId);
		try {
			result = query.executeUpdate();
		} catch (Exception e) {
			LOG.info("Exception occured for activateArchievedSub : " + e);
		}
		return result;

	}

	@Override
	public DataAndCountDto getSubsEmployerList(Long empId, Long siteId, String isPriUser, Long employerUser,
			Long offset, String searchChars, Set<Long> subsIds, Set<Long> siteIds, Set<Long> allSubsIds,
			boolean isSubList) {
		DataAndCountDto dataAndCountDto = new DataAndCountDto();
		try {
			StringBuffer queryString = new StringBuffer(
					"SELECT DISTINCT e.`id`,e.employer_name AS subName,e.address1,e.address2,e.city,e.state,e.zip_code AS zip,e.`latitude`,e.`longitude`,eu.phone_number AS phoneNo,eu.`country_code` AS countryCode ");
			String queryString2 = new String();
			if (siteId == 0) {
				if (null != isPriUser && isPriUser.equalsIgnoreCase("N")) {
					if (!CollectionUtils.isEmpty(siteIds) && !CollectionUtils.isEmpty(allSubsIds)) {
						queryString2 = "SELECT DISTINCT e.`id`,e.subName,e.address1,e.address2,e.city,e.state,e.zip,e.`latitude`,e.`longitude`,e.phoneNo,e.countryCode\n"
								+ "FROM(\n"
								+ "SELECT DISTINCT e.`id`,e.employer_name AS subName,e.address1,e.address2,e.city,e.state,e.zip_code AS zip,e.`latitude`,e.`longitude`,\n"
								+ "	eu.phone_number AS phoneNo,eu.`country_code` AS countryCode \n"
								+ "FROM `project_key_person_entity` pkp \n"
								+ "INNER JOIN projects p ON pkp.`site_id`=p.`id` \n"
								+ "INNER JOIN `project_sub_employer_mapping` pke ON p.id=pke.site_id \n"
								+ "INNER JOIN employer e ON pke.sub_id = e.`id` \n"
								+ "INNER JOIN employer_user eu ON e.`id`=eu.`employer_id` \n"
								+ "WHERE pkp.ssid_access = TRUE AND pkp.`emp_user_id` = " + employerUser
								+ " AND p.`employer_id`=" + empId + " AND eu.`primary_user` = 'Y' AND pkp.`site_id` IN("
								+ siteIds.toString().replace("[", "").replace("]", "") + ")\n" + "UNION ALL \n"
								+ "SELECT DISTINCT e.`id`,e.employer_name AS subName,e.address1,e.address2,e.city,e.state,e.zip_code AS zip,e.`latitude`,e.`longitude`,\n"
								+ "	eu.phone_number AS phoneNo,eu.`country_code` AS countryCode\n"
								+ "FROM `employer` AS e \n"
								+ "INNER JOIN employer_user AS eu ON e.`id`=eu.`employer_id`\n" + "WHERE e.`id` IN("
								+ allSubsIds.toString().replace("[", "").replace("]", "")
								+ ") AND eu.`primary_user`='Y') AS e";
					} else if (!CollectionUtils.isEmpty(siteIds) && CollectionUtils.isEmpty(allSubsIds)) {
						queryString2 = "SELECT DISTINCT e.`id`,e.subName,e.address1,e.address2,e.city,e.state,e.zip,e.`latitude`,e.`longitude`,e.phoneNo,e.countryCode\n"
								+ "FROM(SELECT DISTINCT e.`id`,e.employer_name AS subName,e.address1,e.address2,e.city,e.state,e.zip_code AS zip,e.`latitude`,e.`longitude`,\n"
								+ "	eu.phone_number AS phoneNo,eu.`country_code` AS countryCode \n"
								+ "FROM `project_key_person_entity` pkp \n"
								+ "INNER JOIN projects p ON pkp.`site_id`=p.`id` \n"
								+ "INNER JOIN `project_sub_employer_mapping` pke ON p.id=pke.site_id \n"
								+ "INNER JOIN employer e ON pke.sub_id = e.`id` \n"
								+ "INNER JOIN employer_user eu ON e.`id`=eu.`employer_id` \n"
								+ "WHERE pkp.ssid_access = TRUE AND pkp.`emp_user_id` = " + employerUser
								+ " AND p.`employer_id`=" + empId + " AND eu.`primary_user` = 'Y' AND pkp.`site_id` IN("
								+ siteIds.toString().replace("[", "").replace("]", "") + ")) AS e";
					} else if (CollectionUtils.isEmpty(siteIds) && !CollectionUtils.isEmpty(allSubsIds)) {
						queryString2 = "SELECT DISTINCT e.`id`,e.subName,e.address1,e.address2,e.city,e.state,e.zip,e.`latitude`,e.`longitude`,e.phoneNo,e.countryCode\n"
								+ "FROM(SELECT DISTINCT e.`id`,e.employer_name AS subName,e.address1,e.address2,e.city,e.state,e.zip_code AS zip,e.`latitude`,e.`longitude`,\n"
								+ "	eu.phone_number AS phoneNo,eu.`country_code` AS countryCode\n"
								+ "FROM `employer` AS e \n"
								+ "INNER JOIN employer_user AS eu ON e.`id`=eu.`employer_id`\n" + "WHERE e.`id` IN("
								+ allSubsIds.toString().replace("[", "").replace("]", "")
								+ ") AND eu.`primary_user`='Y') AS e";
					}
				} else {
					queryString.append("FROM vw_subs v INNER JOIN employer AS e ON v.`subs_id`=e.`id` ");
					queryString
							.append("INNER JOIN employer_user AS eu ON e.`id`=eu.`employer_id` WHERE eu.`primary_user`='Y' AND v.`id`="
									+ empId + "");
				}
			} else {
				if (CollectionUtils.isEmpty(subsIds)) {
					queryString.append(
							"FROM project_sub_employer_mapping v INNER JOIN employer AS e ON v.`sub_id`=e.`id` ");
					queryString
							.append("INNER JOIN employer_user AS eu ON e.`id`=eu.`employer_id` WHERE eu.`primary_user`='Y' AND v.site_id="
									+ siteId + " AND v.`employer_id`=" + empId + "");
				} else {
					queryString
							.append("FROM `employer` AS e INNER JOIN `employer_user` AS eu ON e.`id`=eu.`employer_id` WHERE eu.`primary_user`='Y' AND e.`id` IN("
									+ subsIds.toString().replace("[", "").replace("]", "") + ")");
				}
			}

			if (siteId == 0 && null != isPriUser && isPriUser.equalsIgnoreCase("N")) {

				queryString2 = queryString2 + " GROUP BY e.id";
				Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString2);
				query.setResultTransformer(new AliasToBeanResultTransformer(SubForModuleDto.class));
				List<SubForModuleDto> subsList = query.list();
				dataAndCountDto.setSubsData(subsList);
			} else {

				queryString.append(" GROUP BY e.id");
				Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString.toString());
				query.setResultTransformer(new AliasToBeanResultTransformer(SubForModuleDto.class));
				List<SubForModuleDto> subsList = query.list();
				dataAndCountDto.setSubsData(subsList);
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.info("Exception in getSubsEmployerList. " + e);
			for (int i = 0; i <= e.getStackTrace().length - 1; i++) {
				LOG.info(e.getStackTrace()[i].toString());
			}
		}
		return dataAndCountDto;
	}

	@Override
	public List<WorkerDocMapping> getWorkerSiteDocsNew(Long employerId, Long workerId, Long siteId) {
		List<WorkerDocMapping> workerDocMappings = new ArrayList<WorkerDocMapping>();
		try {
			final String queryString = "SELECT * FROM `worker_doc_mapping` WHERE `employer_id`=" + employerId
					+ " AND `worker_id`=" + workerId + " AND `site_id`=" + siteId + "";
			Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
					.addEntity(WorkerDocMapping.class);
			workerDocMappings = query.list();
		} catch (Exception e) {
			e.printStackTrace();
			LOG.info("Exception in getWorkerSiteDocsNew. " + e);
		}
		return workerDocMappings;
	}

	@Override
	public List<NameIdDto> getAllEmpsIdName() {
		List<NameIdDto> subsList = new ArrayList<NameIdDto>();
		try {
			String queryStr = "SELECT id, `employer_name` AS `name` FROM `employer` ORDER BY `employer_name`;";
			Query query = sessionFactory.getCurrentSession().createSQLQuery(queryStr)
					.addScalar("id", StandardBasicTypes.LONG).addScalar("name", StandardBasicTypes.TEXT)
					.setResultTransformer(new AliasToBeanResultTransformer(NameIdDto.class));
			subsList = query.list();
		} catch (Exception e) {
			e.printStackTrace();
			LOG.info("Exception in getAllEmpsIdName. " + e);
		}
		return subsList;
	}

	@Override
	public List<DocumentLibrary> getAllPDFPathfromLibrary() {
		return sessionFactory.getCurrentSession()
				.createSQLQuery(
						"select * from document_library where  doc_type='d' " + "   AND document_path IS NOT NULL ")
				.addEntity(DocumentLibrary.class).list();
	}

	@Override
	public List<EmployerUser> getEmployerPrimaryUsersList(Long empId) {

		final String queryString = "select usr from EmployerUser usr where usr.employerEntity.id=:empId and primary_user='Y'"
				+ "order by usr.employerEntity.employerName asc";
		Query query = sessionFactory.getCurrentSession().createQuery(queryString).setParameter("empId", empId);
		return query.list();

	}

	@Override
	public List<WorkerDocMapping> getWorkersConfirmedDocsByRules(Long workerId, Long employerId,
			List<Long> siteIdRequired) {
		List<WorkerDocMapping> listOfWorkerMapping = new ArrayList<>();
		Query query1 = null;
		final String queryString1 = "select profile from SkillSignalProfileEntity profile where profile.id =:id";
		Query query = sessionFactory.getCurrentSession().createQuery(queryString1).setParameter("id", workerId);
		SkillSignalProfileEntity skillSignalProfileEntity = (SkillSignalProfileEntity) query.uniqueResult();
		HashSet<Long> uniquelistOfRuleId = new HashSet<>();
		if (skillSignalProfileEntity.getEmployerEntity().getId().equals(employerId)) {
			// Own worker

			final String ruleQuery = "SELECT * FROM rule r WHERE r.`actions`='Fill' AND r.`site_id`  IN (:siteIdRequired)   AND (r.user_type='O' OR r.user_type='W' OR r.user_type='O,S')";

			Query rule = ((SQLQuery) sessionFactory.getCurrentSession().createSQLQuery(ruleQuery)
					.setParameterList("siteIdRequired", siteIdRequired)).addEntity(RuleEntity.class);
			List<RuleEntity> listOfRule = rule.list();

			if (!CollectionUtils.isEmpty(listOfRule)) {
				for (RuleEntity ruleEntity : listOfRule) {
					if (StringUtils.isNotBlank(ruleEntity.getValueIds())) {
						String[] ruleId = ruleEntity.getValueIds().split(",");

						for (String ruleIdValue : ruleId) {
							uniquelistOfRuleId.add(Long.valueOf(ruleIdValue.trim()));
						}
					}
				}

				if (uniquelistOfRuleId.size() > 0) {
					final String queryString = "SELECT * FROM `worker_doc_mapping` WHERE doc_lib_id IN (:uniquelistOfRuleId)  AND  `worker_id`="
							+ workerId + " AND `status`IN('Confirmed')";
					query1 = ((SQLQuery) sessionFactory.getCurrentSession().createSQLQuery(queryString)
							.setParameterList("uniquelistOfRuleId", uniquelistOfRuleId))
									.addEntity(WorkerDocMapping.class);

					listOfWorkerMapping = query1.list();

				}

			}

		} else {
			String ruleQuery = null;
			if (null != skillSignalProfileEntity.getEmployerEntity()) {
				ruleQuery = "SELECT * FROM rule r WHERE r.`actions`='Fill' AND r.`site_id` IN (:siteIdRequired)  AND (r.user_type='S' OR r.user_type='W' OR r.user_type='O,S')   AND (   FIND_IN_SET("
						+ skillSignalProfileEntity.getEmployerEntity().getId()
						+ ",r.subs_ids )    OR r.subs_ids IS  NULL)";

			} else {
				ruleQuery = "SELECT * FROM rule r WHERE r.`actions`='Fill' AND r.`site_id` IN (:siteIdRequired)  AND (r.user_type='S' OR r.user_type='W' OR r.user_type='O,S')   AND  r.subs_ids IS  NULL";
			}

			Query rule = ((SQLQuery) sessionFactory.getCurrentSession().createSQLQuery(ruleQuery)
					.setParameterList("siteIdRequired", siteIdRequired)).addEntity(RuleEntity.class);
			List<RuleEntity> listOfRule = rule.list();

			if (!CollectionUtils.isEmpty(listOfRule)) {
				for (RuleEntity ruleEntity : listOfRule) {
					if (StringUtils.isNotBlank(ruleEntity.getValueIds())) {
						String[] ruleId = ruleEntity.getValueIds().split(",");

						for (String ruleIdValue : ruleId) {
							uniquelistOfRuleId.add(Long.valueOf(ruleIdValue.trim()));
						}
					}
				}
				if (uniquelistOfRuleId.size() > 0) {
					final String queryString = "SELECT * FROM `worker_doc_mapping` WHERE doc_lib_id  IN (:uniquelistOfRuleId)  AND  `worker_id`="
							+ workerId + " AND `status`IN('Confirmed')";
					query1 = ((SQLQuery) sessionFactory.getCurrentSession().createSQLQuery(queryString)
							.setParameterList("uniquelistOfRuleId", uniquelistOfRuleId))
									.addEntity(WorkerDocMapping.class);

					listOfWorkerMapping = query1.list();

				}
			}
		}

		return listOfWorkerMapping;
	}

	/**
	 * This API is to get all worker details based on site and date
	 * 
	 * @author Tanay Saxena
	 * @since Feb 25, 2021
	 */

	public List<DemographicReportDto> getWorkersForDemographicReport(long siteId, String date, long employerId,
			long empId) {
		List<DemographicReportDto> demographicReportList = new ArrayList<DemographicReportDto>();
		StringBuilder sb = new StringBuilder(" ");
		if (employerId == empId) {
			sb.append(
					"SELECT DISTINCT s.id, s.first_name AS firstName, s.last_name AS lastName, s.mobile_number AS mobileNumber, s.gender, s.ethnicity, \r\n"
							+ "	s.is_veteran AS isVeteran, s.is_disabled AS isDisabled, s.classification,\r\n"
							+ "	e.employer_name AS employerName ,IF(s.`postal_code` IS NULL,'NA',s.`postal_code`) AS zipCode  \r\n" + "FROM skillsignal_profile AS s \r\n"
							+ "INNER JOIN attendance AS a ON a.worker_id=s.id \r\n"
							+ "INNER JOIN employer AS e ON s.employer_id=e.id \r\n" + "WHERE a.site_id=" + siteId
							+ " AND DATE(a.updated_date)='" + date + "' \r\n"
							+ "GROUP BY s.id ORDER BY s.first_name ASC");
		} else {
			sb.append(
					"SELECT DISTINCT s.id, s.first_name AS firstName, s.last_name AS lastName, s.mobile_number AS mobileNumber, s.gender, s.ethnicity, \r\n"
							+ "	s.is_veteran AS isVeteran, s.is_disabled AS isDisabled, s.classification, \r\n"
							+ " e.employer_name AS employerName , IF(s.`postal_code` IS NULL,'NA',s.`postal_code`) AS zipCode \r\n"
							+ "FROM `project_workers_mapping` AS pwm \r\n"
							+ "INNER JOIN skillsignal_profile AS s ON s.`id`=pwm.worker_id \r\n"
							+ "INNER JOIN attendance AS a ON a.worker_id=s.id \r\n"
							+ "INNER JOIN employer AS e ON s.employer_id=e.id \r\n" + "WHERE a.site_id=" + siteId
							+ " AND pwm.site_id=" + siteId + " AND pwm.subs_id=" + employerId
							+ " AND DATE(a.updated_date)='" + date + "' \r\n"
							+ "GROUP BY s.id ORDER BY s.first_name ASC");
		}
		Query query = sessionFactory.getCurrentSession().createSQLQuery(sb.toString())
				.addScalar("id", StandardBasicTypes.LONG).addScalar("firstName", StandardBasicTypes.TEXT)
				.addScalar("lastName", StandardBasicTypes.TEXT).addScalar("mobileNumber", StandardBasicTypes.LONG)
				.addScalar("gender", StandardBasicTypes.TEXT).addScalar("ethnicity", StandardBasicTypes.TEXT)
				.addScalar("isVeteran", StandardBasicTypes.TEXT).addScalar("isDisabled", StandardBasicTypes.TEXT)
				.addScalar("classification", StandardBasicTypes.TEXT).addScalar("employerName", StandardBasicTypes.TEXT)
				.addScalar("zipCode", StandardBasicTypes.TEXT)
				.setResultTransformer(new AliasToBeanResultTransformer(DemographicReportDto.class));

		demographicReportList = query.list();
		return demographicReportList;
	}

	@Override
	public List<EmployerEntity> getPaidUnpaidEmployers(String isPaid) {
		String queryString = new String();
		if (StringUtils.isEmpty(isPaid)) {
			queryString = "SELECT * FROM employer;";
		} else {
			if (isPaid.equals("Y")) {
				queryString = "SELECT * FROM employer WHERE is_paid='" + isPaid + "'";
			} else {
				queryString = "SELECT * FROM employer WHERE is_paid='" + isPaid + "' OR is_paid IS NULL";
			}
		}
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(EmployerEntity.class);
		return query.list();
	}

	public List<DemographicReportDto> getClockInClockOutTime(long workerId, long siteId) {
		List<DemographicReportDto> demographicReport = new ArrayList<DemographicReportDto>();
		StringBuilder sb = new StringBuilder(" ");
		sb.append(" SELECT DISTINCT s.id, s.first_name AS firstName,\r\n"
				+ " MIN(DATE(a.clock_in_time)) AS clockInTime,\r\n" + " MAX(DATE(a.clock_in_time)) AS clockOutTime\r\n"
				+ " FROM skillsignal_profile AS s\r\n" + "	INNER JOIN attendance AS a ON a.worker_id=s.id\r\n"
				+ "				INNER JOIN employer AS e ON s.employer_id=e.id\r\n" + "				WHERE a.site_id="
				+ siteId + " AND worker_id=" + workerId + "\r\n"
				+ "				GROUP BY s.id ORDER BY s.first_name ASC ");

		Query query = sessionFactory.getCurrentSession().createSQLQuery(sb.toString())
				.addScalar("id", StandardBasicTypes.LONG).addScalar("firstName", StandardBasicTypes.TEXT)
				.addScalar("clockInTime", StandardBasicTypes.TEXT).addScalar("clockOutTime", StandardBasicTypes.TEXT)
				.setResultTransformer(new AliasToBeanResultTransformer(DemographicReportDto.class));

		demographicReport = query.list();
		return demographicReport;

	}

	public String getTradeBySiteId(long workerId, long siteId) {
		List<DemographicReportDto> demographicReport = new ArrayList<DemographicReportDto>();
		StringBuilder sb = new StringBuilder(" ");
		sb.append(" SELECT pwm.trade FROM project_workers_mapping AS pwm WHERE pwm.worker_id=" + workerId
				+ " AND pwm.site_id=" + siteId + " order by pwm.id desc limit 1");
		Query query = sessionFactory.getCurrentSession().createSQLQuery(sb.toString());
		// demographicReport = query.list();
		return (String) query.uniqueResult();

	}

	public List<DailyReportDto> getDailyReport(String date) {
		List<DailyReportDto> dailyReport = new ArrayList<DailyReportDto>();
		StringBuilder sb = new StringBuilder("");
		sb.append(
				" SELECT DISTINCT a.worker_id AS workerId,a.site_id as siteId, p.employer_name AS employerName, a.site_name AS siteName, "
						+ " COUNT(DISTINCT a.worker_id) AS totalClockedInWorker, COUNT(DISTINCT ws.id) AS surveySubmitted \r\n"
						+ " FROM attendance AS a \r\n"
						+ " INNER JOIN projects AS p ON p.id=a.site_id LEFT JOIN worker_survey AS ws ON a.worker_id=ws.worker_id "
						+ " AND DATE(a.updated_date)=DATE(ws.updated_date) AND a.site_id=ws.site_id \r\n"
						+ " WHERE DATE(a.updated_date)='" + date
						+ "' AND p.`employer_id` NOT IN (5512,80983, 80986, 80937, 81067, 81059, 81104, 80883, 81296, 10213, 80897, 80911, 81000, 81295, 81063, 80935, 81005) "
						+ " GROUP BY a.site_id ORDER BY p.employer_name DESC ");

		Query query = sessionFactory.getCurrentSession().createSQLQuery(sb.toString())
				.addScalar("workerId", StandardBasicTypes.LONG).addScalar("siteId", StandardBasicTypes.LONG)
				.addScalar("employerName", StandardBasicTypes.TEXT).addScalar("siteName", StandardBasicTypes.TEXT)
				.addScalar("totalClockedInWorker", StandardBasicTypes.LONG)
				.addScalar("surveySubmitted", StandardBasicTypes.LONG)
				.setResultTransformer(new AliasToBeanResultTransformer(DailyReportDto.class));
		dailyReport = query.list();
		return dailyReport;
	}

	public long getVisitorCount(String date, long siteId) {
		SimpleDateFormat sdfIn = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat sdfOut = new SimpleDateFormat("MM/dd/yyyy");

		Date dateFormatChange = null;
		try {
			dateFormatChange = sdfIn.parse(date);
		} catch (Exception e) {
			e.printStackTrace();
		}
		String inputDate = sdfOut.format(dateFormatChange);
		StringBuilder sb = new StringBuilder("");
		sb.append(" SELECT count(ws.id) AS visitorId \r\n" + " FROM `visitor` as ws WHERE ws.created_date='" + inputDate
				+ "' AND ws.site_id=" + siteId + " ");

		Query query = sessionFactory.getCurrentSession().createSQLQuery(sb.toString()).addScalar("visitorId",
				StandardBasicTypes.LONG);
		return (long) query.uniqueResult();

	}

	public List<DailyReportDto> getWorkersNotSubmittedSurvey(String date) {
		List<DailyReportDto> dailyReport = new ArrayList<DailyReportDto>();
		StringBuilder sb = new StringBuilder("");
		sb.append(
				" SELECT DISTINCT a.worker_id AS workerId,a.site_name AS siteName,s.first_name AS firstName,s.last_name AS lastName,"
						+ " s.mobile_number AS mobileNumber FROM attendance \r\n"
						+ "	AS a INNER JOIN skillsignal_profile AS s ON s.id=a.worker_id \r\n"
						+ "	LEFT JOIN worker_survey AS ws ON a.worker_id=ws.worker_id AND DATE(a.updated_date)=DATE(ws.updated_date) AND a.site_id=ws.site_id \r\n"
						+ "	WHERE DATE(a.updated_date)='" + date + "' \r\n" + "	AND ws.worker_id IS NULL \r\n"
						+ " GROUP BY a.`worker_id` ORDER BY a.`worker_id` ASC ");

		Query query = sessionFactory.getCurrentSession().createSQLQuery(sb.toString())
				.addScalar("workerId", StandardBasicTypes.LONG).addScalar("siteName", StandardBasicTypes.TEXT)
				.addScalar("firstName", StandardBasicTypes.TEXT).addScalar("lastName", StandardBasicTypes.TEXT)
				.addScalar("mobileNumber", StandardBasicTypes.LONG)
				.setResultTransformer(new AliasToBeanResultTransformer(DailyReportDto.class));

		dailyReport = query.list();
		return dailyReport;

	}

	public List<WeeklyReportStatusDto> getSiteAllWorkerCount(String fromDate, String toDate, long siteId) {
		List<WeeklyReportStatusDto> weeklyReport = new ArrayList<WeeklyReportStatusDto>();

		StringBuilder sb = new StringBuilder("");

		sb.append(" SELECT DATE(a.`updated_date`) as updatedDate, \r\n"
				+ "COUNT(DISTINCT a.worker_id) AS totalWorkerCount FROM attendance AS a WHERE \r\n"
				+ " DATE(a.updated_date) BETWEEN '" + fromDate + "' AND '" + toDate + "' \r\n" + " AND a.site_id="
				+ siteId + " GROUP BY DATE(a.updated_date) ");

		Query query = sessionFactory.getCurrentSession().createSQLQuery(sb.toString())
				.addScalar("updatedDate", StandardBasicTypes.TEXT)
				.addScalar("totalWorkerCount", StandardBasicTypes.LONG)
				.setResultTransformer(new AliasToBeanResultTransformer(WeeklyReportStatusDto.class));

		weeklyReport = query.list();
		return weeklyReport;

	}

	public List<WeeklyReportStatusDto> getEmployerAllSites(long employerId) {
		List<WeeklyReportStatusDto> weeklyReportDto = new ArrayList<WeeklyReportStatusDto>();

		StringBuilder sb = new StringBuilder("");

		sb.append(
				" SELECT  DISTINCT p.id AS siteId,p.name AS siteName, p.employer_name AS employerName FROM projects AS p"
						+ " WHERE p.`employer_id`=" + employerId + " ORDER BY p.name ASC \r\n ");

		Query query = sessionFactory.getCurrentSession().createSQLQuery(sb.toString())
				.addScalar("siteId", StandardBasicTypes.LONG).addScalar("siteName", StandardBasicTypes.TEXT)
				.addScalar("employerName", StandardBasicTypes.TEXT)
				.setResultTransformer(new AliasToBeanResultTransformer(WeeklyReportStatusDto.class));
		weeklyReportDto = query.list();
		return weeklyReportDto;

	}

	public List<DemographicReportDto> getWorkersForSiteDemographicReport(long siteId, String fromDate, String tillDate,
			long employerId, long empId) {
		List<DemographicReportDto> demographicReportList = new ArrayList<DemographicReportDto>();
		StringBuilder sb = new StringBuilder("CALL GET_DATA_FOR_SITE_DEMOGRAPHIC_REPORT(" + siteId + ",'" + fromDate
				+ "','" + tillDate + "'," + employerId + "," + empId + ")");
		Query query = sessionFactory.getCurrentSession().createSQLQuery(sb.toString())
				.addScalar("id", StandardBasicTypes.LONG).addScalar("firstName", StandardBasicTypes.TEXT)
				.addScalar("lastName", StandardBasicTypes.TEXT).addScalar("mobileNumber", StandardBasicTypes.LONG)
				.addScalar("employerName", StandardBasicTypes.TEXT).addScalar("maleCount", StandardBasicTypes.LONG)
				.addScalar("femaleCount", StandardBasicTypes.LONG).addScalar("veteranCount", StandardBasicTypes.TEXT)
				.addScalar("disabledCount", StandardBasicTypes.TEXT).addScalar("asianCount", StandardBasicTypes.LONG)
				.addScalar("blackOrAfricanAmericanCount", StandardBasicTypes.LONG)
				.addScalar("caucasianOrWhiteCount", StandardBasicTypes.LONG)
				.addScalar("hispanicOrLatinoCount", StandardBasicTypes.LONG)
				.addScalar("nativeAmericanCount", StandardBasicTypes.LONG)
				.addScalar("apprenticeFirstYearCount", StandardBasicTypes.LONG)
				.addScalar("apprenticeSecondYearCount", StandardBasicTypes.LONG)
				.addScalar("apprenticeThirdYearCount", StandardBasicTypes.LONG)
				.addScalar("apprenticeFourthYearCount", StandardBasicTypes.LONG)
				.addScalar("apprenticeFifthYearCount", StandardBasicTypes.LONG)
				.addScalar("journeymanCount", StandardBasicTypes.LONG)
				.addScalar("masterCraftsmanCount", StandardBasicTypes.LONG)
				.addScalar("foremanCount", StandardBasicTypes.LONG)
				.addScalar("superIntendentCount", StandardBasicTypes.LONG).addScalar("zipCode", StandardBasicTypes.TEXT)
				.setResultTransformer(new AliasToBeanResultTransformer(DemographicReportDto.class));

		demographicReportList = query.list();
		return demographicReportList;
	}

	public List<Long> getAllWorkersNightShift(String date, long siteId) {

		StringBuilder sb = new StringBuilder("");

		sb.append(" SELECT DISTINCT a.worker_id AS workerId FROM attendance AS a LEFT JOIN \r\n"
				+ "	worker_survey AS ws  ON ws.worker_id=a.worker_id AND \r\n"
				+ " DATE(ws.updated_date)=DATE(a.updated_date)	AND a.`site_id`= ws.`site_id`    \r\n"
				+ " WHERE a.`site_id`=" + siteId + " AND DATE(a.updated_date)='" + date + "' \r\n"
				+ "	     AND  ws.`worker_id` IS  NULL ORDER BY a.worker_id ");

		Query query = sessionFactory.getCurrentSession().createSQLQuery(sb.toString()).addScalar("workerId",
				StandardBasicTypes.LONG);
		List<Long> workerIdList = query.list();
		return workerIdList;
	}

	public long getWorkerBySiteId(long workerId, String date, long siteId) {
		StringBuilder sb = new StringBuilder("");
		sb = sb.append("  SELECT count(*) as workerCount FROM attendance AS a  WHERE a.worker_id=" + workerId
				+ " AND DATE(a.updated_date)='" + date + "'  AND a.site_id=" + siteId + " \r\n"
				+ "	AND a.clock_out_time='" + date + " 23:59:59'");
		Query query = sessionFactory.getCurrentSession().createSQLQuery(sb.toString()).addScalar("workerCount",
				StandardBasicTypes.LONG);
		return (long) query.uniqueResult();
	}

	@Override
	public List<Long> getSubEmployerListbyEmployerIdAndSiteId(Long employerId, Long siteId) throws Exception {
		/***
		 * for sublisting this query is added for crew------ ON e.id=psem.sub_id
		 ***/

		String queryString = "SELECT DISTINCT psem.`sub_id` AS subId FROM `employer_subs_mapping` esm "
				+ "	 INNER JOIN  project_sub_employer_mapping psem ON psem.`sub_id`=esm.`subs_id`  "
				+ "  INNER JOIN employer e  " + "  ON e.id=psem.sub_id  " + "  WHERE  e.`id`=" + employerId
				+ " AND psem.`site_id`=" + siteId + " ORDER BY sub_id ASC ";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addScalar("subId",
				StandardBasicTypes.LONG);

		return query.list();
	}

	@Override
	public List<Long> getWorkerListForNightShift(String previousDate) {
		String queryString = "SELECT a.`worker_id` as workerId  FROM attendance AS a  WHERE  DATE(a.updated_date)='"
				+ previousDate + "'   \r\n" + "	AND a.clock_out_time='" + previousDate + " 23:59:59'";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString.toString()).addScalar("workerId",
				StandardBasicTypes.LONG);
		return query.list();
	}

	// added for first day on site
	public String getFirstDayOnSite(String siteCode, long workerId) {

		StringBuilder sb = new StringBuilder(" ");
		sb.append(
				"SELECT DATE_FORMAT(MIN(STR_TO_DATE(a.updated_date, '%Y-%m-%d %H:%i:%s')), '%Y-%m-%d %H:%i:%s') AS firstDayOnSite \n "
						+ " FROM   attendance a\n" + "       JOIN project_workers_mapping pwm\n"
						+ "         ON a.worker_id = pwm.worker_id\n" + "            AND a.site_id = pwm.site_id\n"
						+ "            AND pwm.worker_active = 'Y' \n" + "       JOIN projects p\n"
						+ "         ON p.id = a.site_id\n" + " WHERE  p.site_code = :siteCode \n"
						+ "       AND pwm.worker_id = :workerId \n" + " ORDER  BY a.id ASC limit 1 ");
		Query query = sessionFactory.getCurrentSession().createSQLQuery(sb.toString())
				.setParameter("siteCode", siteCode).setParameter("workerId", workerId);

		return (String) query.uniqueResult();

	}

	@Override
	public List<DocumentType> getDocumentTypeList() {
		Criteria criteria = sessionFactory.getCurrentSession().createCriteria(DocumentType.class);
		criteria.addOrder(Order.asc("id"));
		return criteria.list();
	}
	
	//archive template start
	@Override
	public List<DocumentLibrary> getArchiveTemplateLibrary(Long empId, String docType) {
		return sessionFactory.getCurrentSession()
				.createSQLQuery("select * from document_library where employer_id=" + empId + " AND doc_type='"
						+ docType + "' AND sub_employer_id IS NULL AND worker_id	IS NULL AND is_active=FALSE")
				.addEntity(DocumentLibrary.class).list();
	}
	//archive template end

	//added for redirection to url for requirement start
	@Override
	public List<LibraryListForApp> getDetailOfForVideoRedirection(Long employerId, Long workerId, Long siteId,Long docId) {
		List<LibraryListForApp> libraryListForApps = new ArrayList<LibraryListForApp>();

		LOG.info(" inside getDetailOfVideoForRedirection");
		try
		{
			final String queryString = "SELECT DISTINCT dl.id                     AS id," +
					"                dl.`added_by`             AS publishedBy," +
					"                dl.`document_description` AS docDesc," +
					"                dl.`document_name`        AS docTitle," +
					"                dl.`document_path`        AS docLink," +
					"                dlm.`is_seen`             AS isSeen," +
					"                dlm.`is_ack`              AS isAck," +
					"                dl.thumbnail_path         AS thumbnailPath," +
					"                dlm.`seen_date`           AS seenDate" +
					" FROM   document_library dl" +
					"       INNER JOIN `doc_lib_mapping` dlm" +
					"               ON dl.id = dlm.`doc_lib_id`" +
					"WHERE  dlm.`worker_id` = '"+workerId+"'" +
					"       AND dl.`doc_type` = 'v'" +
					"       AND dl.`is_active` = true" +
					"       AND dl.id ='"+docId+"' ";
					
					LOG.info("Query for getDetailOfVideoForRedirection ::"+queryString);
					Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
							.addScalar("id", StandardBasicTypes.LONG)
							.addScalar("publishedBy", StandardBasicTypes.TEXT)
							.addScalar("docDesc", StandardBasicTypes.TEXT)
							.addScalar("docTitle", StandardBasicTypes.TEXT)
							.addScalar("docLink", StandardBasicTypes.TEXT)
							.addScalar("isSeen", StandardBasicTypes.TEXT)
							.addScalar("isAck", StandardBasicTypes.TEXT)
							.addScalar("thumbnailPath", StandardBasicTypes.TEXT)
							.addScalar("seenDate", StandardBasicTypes.TEXT)
							.setResultTransformer(new AliasToBeanResultTransformer(LibraryListForApp.class));

					libraryListForApps = query.list();

		}
		catch(Exception e)
		{
			LOG.info("Exception in getDetailOfVideoForRedirection "+e);
			e.printStackTrace();
		}

				return libraryListForApps;
			
	}
	
	@Override
	public List<WorkerDocListDto> getDetailOfForDocRedirection(Long employerId, Long workerId, Long siteId,Long docId) {
		List<WorkerDocListDto> workerDocListForApps = new ArrayList<WorkerDocListDto>();

		LOG.info(" inside getDetailOfForDocRedirection");
		try
		{
			String queryString = "Select a.docMapId,a.createdDate,a.docDesc,a.docName,a.docPath,a.userDocName,a.status,a.siteName,a.empName,a.employerId,a.docPageCount from (SELECT id AS docMapId, created_date AS createdDate, document_description AS docDesc, "
					+ "document_name AS docName, document_path AS docPath, user_document_name AS userDocName, "
					+ "'Uploaded' AS `status`, 'NA' AS siteName, 'NA' AS empName, employer_id AS `employerId`, doc_page_count AS docPageCount FROM document_library "
					+ "WHERE worker_id=" + workerId
					+ " UNION ALL SELECT wdm.id AS docMapId, dl.created_date AS createdDate, "
					+ "dl.document_description AS docDesc, dl.document_name AS docName, wdm.document_path AS docPath, "
					+ "dl.user_document_name AS userDocName, wdm.`status`, p.name AS siteName, e.employer_name AS empName, p.employer_id AS employerId, dl.doc_page_count AS docPageCount "
					+ "FROM document_library dl INNER JOIN worker_doc_mapping wdm ON dl.id=wdm.doc_lib_id "
					+ "INNER JOIN projects p ON wdm.site_id=p.id INNER JOIN employer e ON p.employer_id=e.id "
					+ "WHERE wdm.worker_id=" + workerId + " AND wdm.status NOT IN('Deleted')) a where a.docMapId='"+docId+"'";

			LOG.info("Query for getDetailOfForDocRedirection ::"+queryString);
			Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
					.setResultTransformer(new AliasToBeanResultTransformer(WorkerDocListDto.class));
			workerDocListForApps= query.list();

		}
		catch(Exception e)
		{
			LOG.info("Exception in getDetailOfVideoForRedirection "+e);
			e.printStackTrace();
		}

				return workerDocListForApps;
			
	}
	
	//added for redirection to url for requirement end
	@Override
	public List<Long>  getSubSitesForloginEmp(long empId) {
		String queryString =" SELECT pwm.site_id as siteId FROM project_workers_mapping AS pwm WHERE " 
				+ " pwm.subs_id=" + empId; 
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString.toString()).addScalar("siteId",
				StandardBasicTypes.LONG);
		return query.list();

	}

	@Override
	public List<DailyReportNotes> getNotesByEmpIdNew(Long id, String today, String projectIds) throws Exception  {
		final String queryString = "SELECT * FROM daily_report_notes WHERE site_id IN("+projectIds
				+ ") AND DATE(`time_stamp`)='" + today + "';";
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString).addEntity(DailyReportNotes.class);
		return query.list();
	}
	

	//Ibc Batch In Out for IbcReport get own and assigned site start
	@Override
	public List<NameIdDto> getIBCOwnAssignedProjects(Long empId,boolean isIBCDailyReport,EmployerUser empUser) {
	
		String queryString="";
		if (empUser.getPrimaryUser().equalsIgnoreCase("Y") || empUser.getPrimaryUser().equalsIgnoreCase("X")) 
		{
			 queryString = "SELECT DISTINCT a.id, a.name, a.siteCode, a.employerId, a.createdDate,  a.gpsTrackingFlag  \n"
					+ "FROM(SELECT p.id, p.`name`, p.site_code AS siteCode, p.employer_id AS employerId, p.created_date AS createdDate ,  c.gps_tracking_flag AS gpsTrackingFlag "
					+ " FROM projects p " + "  INNER JOIN configuration c  ON c.id=p.configuration_id  "
					+ " WHERE employer_id=" + empId + "  AND p.`is_active`='Y' AND p.turnstile_job_id IS NOT NULL \n" + "	UNION ALL\n"
					+ "	SELECT p.id,p.`name`,p.site_code AS siteCode,p.employer_id AS employerId, p.created_date AS createdDate , c.gps_tracking_flag AS gpsTrackingFlag "
					+ "FROM `projects` p\n" + "  INNER JOIN configuration c  ON c.id=p.configuration_id  "
					+ "	INNER JOIN `project_sub_employer_mapping` psem ON p.`id` = psem.`site_id` \n"
					+ "	WHERE  psem.`sub_id`=" + empId + "  AND p.`is_active`='Y' AND p.turnstile_job_id IS NOT NULL \n" + "	UNION ALL\n"
					+ "	SELECT p.id,p.`name`,p.site_code AS siteCode,p.employer_id AS employerId, p.created_date AS createdDate , c.gps_tracking_flag AS gpsTrackingFlag "
					+ "FROM `projects` p \n" + "  INNER JOIN configuration c  ON c.id=p.configuration_id  "
					+ "	INNER JOIN `project_key_person_entity` pke ON p.`id`=pke.`site_id` \n"
					+ "	INNER JOIN `employer_user` eu ON eu.id=pke.`emp_user_id` \n" + " WHERE eu.employer_id=" + empId
					+ " AND `ssid_access` IS TRUE  AND p.`is_active`='Y' AND p.turnstile_job_id IS NOT NULL ) AS a ";
						queryString=queryString+ " WHERE a.id NOT IN (SELECT DISTINCT site_id FROM project_archive where employer_user_id ="+empUser.getId() +" and employer_id="+empUser.getEmployerEntity().getId()+" ) ";
					
					queryString=queryString+ " \n " + "ORDER BY a.name ASC";
			
					LOG.info(" for ibc project list for report for primary user "+queryString);
		}
		else
		{
			 queryString = "SELECT p.id,p.`name` ,p.site_code AS siteCode, p.employer_id AS employerId, p.created_date AS createdDate, false AS gpsTrackingFlag FROM projects p \n"
					+ "INNER JOIN `project_key_person_entity` pke ON p.`id`=pke.`site_id`\n"
					+ "WHERE pke.`emp_user_id`=" + empUser.getId() + " AND pke.ssid_access=TRUE AND p.turnstile_job_id IS NOT NULL \n"
					+ "ORDER BY p.`name` ASC, p.`is_active` DESC";
			
			 LOG.info(" for ibc project list for report for not primary user "+queryString);
			
		}
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
					.addScalar("id", StandardBasicTypes.LONG).addScalar("name", StandardBasicTypes.TEXT)
					.addScalar("siteCode", StandardBasicTypes.TEXT).addScalar("employerId", StandardBasicTypes.LONG)
					.addScalar("createdDate", StandardBasicTypes.TEXT)
					.addScalar("gpsTrackingFlag", StandardBasicTypes.BOOLEAN)
					.setResultTransformer(new AliasToBeanResultTransformer(NameIdDto.class));

		return query.list();
	}
	//Ibc Batch In Out for IbcReport get own and assigned site end
	
	//for subcontractor daily report start
	@Override
	public List<DailyReportNotesSubContractorDTO> getNotesByEmpIdSubContractor(Long id, String today, String projectIds) throws Exception  {
		
		final String queryString="SELECT drn.phone_no AS 'phoneNo',drn.description AS 'description',drn.doc_paths AS 'docPaths',drn.time_stamp AS 'timeStamp',drn.name AS 'name',p.name AS siteName FROM daily_report_notes drn "
				+ " LEFT JOIN projects p "
				+ " ON p.id=drn.site_id "
				+ " WHERE drn.site_id IN("+projectIds+") AND DATE(`time_stamp`)='"+today+"'";
		
		Query query = sessionFactory.getCurrentSession().createSQLQuery(queryString)
				.addScalar("phoneNo", StandardBasicTypes.TEXT)
				.addScalar("description", StandardBasicTypes.TEXT)
				.addScalar("docPaths", StandardBasicTypes.TEXT)
				.addScalar("timeStamp", StandardBasicTypes.TEXT)
				.addScalar("name", StandardBasicTypes.TEXT)
				.addScalar("siteName", StandardBasicTypes.TEXT)
				.setResultTransformer(new AliasToBeanResultTransformer(DailyReportNotesSubContractorDTO.class));
		return query.list();
	}
	//for subcontractor daily report end
}// End
