package net.srt.quartz.api;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import lombok.RequiredArgsConstructor;
import net.srt.api.module.data.integrate.DataAccessApi;
import net.srt.api.module.data.integrate.constant.TaskType;
import net.srt.api.module.data.integrate.dto.DataAccessDto;
import net.srt.api.module.data.integrate.dto.DataAccessTaskDto;
import net.srt.api.module.quartz.QuartzDataAccessApi;
import net.srt.api.module.quartz.constant.QuartzJobType;
import net.srt.framework.common.utils.Result;
import net.srt.quartz.entity.ScheduleJobEntity;
import net.srt.quartz.enums.JobGroupEnum;
import net.srt.quartz.enums.ScheduleConcurrentEnum;
import net.srt.quartz.enums.ScheduleStatusEnum;
import net.srt.quartz.service.ScheduleJobService;
import net.srt.quartz.task.DataAccessTask;
import net.srt.quartz.utils.ScheduleUtils;
import org.quartz.Scheduler;
import org.springframework.web.bind.annotation.RestController;

/**
 * 短信服务API
 *
 * @author 阿沐 babamu@126.com
 */
@RestController
@RequiredArgsConstructor
public class QuartzDataAccessApiImpl implements QuartzDataAccessApi {

	private final Scheduler scheduler;
	private final DataAccessApi dataAccessApi;
	private final ScheduleJobService jobService;

	@Override
	public Result<String> releaseAccess(Long id) {
		ScheduleJobEntity jobEntity = buildJobEntity(id);
		//判断是否存在,不存在，新增，存在，设置主键
		jobService.buildSystemJob(jobEntity);
		ScheduleUtils.createScheduleJob(scheduler, jobEntity);
		return Result.ok();
	}

	@Override
	public Result<String> cancleAccess(Long id) {
		ScheduleJobEntity jobEntity = buildJobEntity(id);
		jobService.buildSystemJob(jobEntity);
		ScheduleUtils.deleteScheduleJob(scheduler, jobEntity);
		//更新任务状态为暂停
		jobService.pauseSystemJob(jobEntity);
		return Result.ok();
	}

	@Override
	public Result<String> handRun(Long id) {
		ScheduleJobEntity jobEntity = buildJobEntity(id);
		jobEntity.setOnce(true);
		jobEntity.setSaveLog(false);
		ScheduleUtils.run(scheduler, jobEntity);
		return Result.ok();
	}


	private ScheduleJobEntity buildJobEntity(Long id) {
		DataAccessDto dataAccessDto = dataAccessApi.getById(id).getData();
		return ScheduleJobEntity.builder().typeId(id).projectId(dataAccessDto.getProjectId()).jobType(QuartzJobType.DATA_ACCESS.getValue()).jobName(String.format("[%s]%s", id.toString(), dataAccessDto.getTaskName())).concurrent(ScheduleConcurrentEnum.NO.getValue())
				.beanName("dataAccessTask").method("run").jobGroup(JobGroupEnum.DATA_ACCESS.getValue()).saveLog(true).cronExpression(dataAccessDto.getCron()).status(ScheduleStatusEnum.NORMAL.getValue())
				.params(String.valueOf(id)).once(TaskType.ONE_TIME_FULL_SYNC.getCode().equals(dataAccessDto.getTaskType())).build();

	}
}
