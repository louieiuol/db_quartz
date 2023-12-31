package net.srt.quartz.task.quality.impl;

import lombok.extern.slf4j.Slf4j;
import net.srt.api.module.data.governance.dto.DataGovernanceQualityTaskColumnDto;
import net.srt.api.module.data.governance.dto.DataGovernanceQualityTaskTableDto;
import net.srt.api.module.data.governance.dto.quality.QualityCheck;
import net.srt.api.module.data.governance.dto.quality.QulaityColumn;
import net.srt.api.module.data.integrate.constant.CommonRunStatus;
import net.srt.flink.common.utils.LogUtil;
import net.srt.quartz.task.quality.AbstractQualityAdapter;
import srt.cloud.framework.dbswitch.common.type.ProductTypeEnum;
import srt.cloud.framework.dbswitch.core.service.IMetaDataByJdbcService;
import srt.cloud.framework.dbswitch.core.service.impl.MetaDataByJdbcServiceImpl;
import srt.cloud.framework.dbswitch.dbcommon.database.DatabaseOperatorFactory;
import srt.cloud.framework.dbswitch.dbcommon.database.IDatabaseOperator;
import srt.cloud.framework.dbswitch.dbcommon.domain.StatementResultSet;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @ClassName UniquenessImpl
 * @Author zrx
 * @Date 2023/6/24 13:13
 */
@Slf4j
public class RegxImpl extends AbstractQualityAdapter {

	private final static Integer BATCH_SIZE = 5000;

	public RegxImpl(QualityCheck qualityCheck) {
		super(qualityCheck);
	}

	@Override
	public void check() {
		log.info(String.format("RegxImpl start check,jdbcUrl:%s,tableName:%s", qualityCheck.getJdbcUrl(), qualityCheck.getTableName()));
		//连接数据库
		ProductTypeEnum productTypeEnum = ProductTypeEnum.getByIndex(qualityCheck.getDatabaseType());
		List<QulaityColumn> qulaityColumns = qualityCheck.getQulaityColumns();
		addTaskColumns(productTypeEnum, qulaityColumns);

	}

	private void addTaskColumns(ProductTypeEnum productTypeEnum, List<QulaityColumn> columnInfos) {
		DataGovernanceQualityTaskTableDto taskTable = addTaskTable(columnInfos);
		try {
			List<String> columns = columnInfos.stream().map(QulaityColumn::getColumnName).collect(Collectors.toList());
			IDatabaseOperator sourceOperator = DatabaseOperatorFactory
					.createDatabaseOperator(createDataSource(qualityCheck), productTypeEnum);
			sourceOperator.setFetchSize(BATCH_SIZE);
			try (StatementResultSet srs = sourceOperator.queryTableData(ProductTypeEnum.ORACLE.equals(productTypeEnum) ? qualityCheck.getUserName() : qualityCheck.getDatabaseName(), qualityCheck.getTableName(), columns); ResultSet rs = srs.getResultset()) {
				int size = 0;
				List<DataGovernanceQualityTaskColumnDto> columnDtos = new ArrayList<>();
				while (rs.next()) {
					size++;
					Map<String, Object> map = buildRowMap(columns, rs);
					//逐个判断是否符合正则表达式
					StringBuilder notPassInfo = new StringBuilder();
					boolean pass = true;
					for (Map.Entry<String, Object> entry : map.entrySet()) {
						String key = entry.getKey();
						Object value = entry.getValue();
						if (value == null) {
							pass = false;
							notPassInfo.append(String.format("【%s】字段不合规；", key));
						} else {
							String vStr = String.valueOf(value);
							if (!vStr.matches(regx)) {
								pass = false;
								notPassInfo.append(String.format("【%s】字段不合规；", key));
							}
						}
					}
					//构建taskColumn
					DataGovernanceQualityTaskColumnDto columnDto = buildTaskColumn(taskTable, map, pass, notPassInfo.length() > 0 ? notPassInfo.deleteCharAt(notPassInfo.length() - 1).toString() : null);
					columnDtos.add(columnDto);
					//5000一次
					if (size % BATCH_SIZE == 0) {
						//更新任务和表任务的检测数
						updateTask(taskTable, columnDtos);
					}
				}
				// 检查剩下没更新的
				if (!columnDtos.isEmpty()) {
					updateTask(taskTable, columnDtos);
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			taskTable.setEndTime(new Date());
			taskTable.setStatus(CommonRunStatus.SUCCESS.getCode());
			dataQualityApi.updateQualityTaskTable(taskTable);
		} catch (Exception e) {
			taskTable.setEndTime(new Date());
			taskTable.setErrorLog(LogUtil.getError(e));
			taskTable.setStatus(CommonRunStatus.FAILED.getCode());
			dataQualityApi.updateQualityTaskTable(taskTable);
			throw new RuntimeException(e);
		}
	}


}
