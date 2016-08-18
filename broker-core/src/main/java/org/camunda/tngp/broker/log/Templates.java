package org.camunda.tngp.broker.log;

import java.util.HashMap;
import java.util.Map;

import org.camunda.tngp.broker.taskqueue.CreateTaskInstanceRequestReader;
import org.camunda.tngp.broker.taskqueue.TaskInstanceReader;
import org.camunda.tngp.broker.taskqueue.log.TaskInstanceRequestReader;
import org.camunda.tngp.broker.wf.repository.log.WfDefinitionReader;
import org.camunda.tngp.broker.wf.repository.log.WfDefinitionRequestReader;
import org.camunda.tngp.broker.wf.runtime.log.ActivityInstanceRequestReader;
import org.camunda.tngp.broker.wf.runtime.log.WfDefinitionRuntimeRequestReader;
import org.camunda.tngp.broker.wf.runtime.log.WorkflowInstanceRequestReader;
import org.camunda.tngp.broker.wf.runtime.log.bpmn.BpmnActivityEventReader;
import org.camunda.tngp.broker.wf.runtime.log.bpmn.BpmnFlowElementEventReader;
import org.camunda.tngp.broker.wf.runtime.log.bpmn.BpmnProcessEventReader;
import org.camunda.tngp.taskqueue.data.ActivityInstanceRequestDecoder;
import org.camunda.tngp.taskqueue.data.BpmnActivityEventDecoder;
import org.camunda.tngp.taskqueue.data.BpmnFlowElementEventDecoder;
import org.camunda.tngp.taskqueue.data.BpmnProcessEventDecoder;
import org.camunda.tngp.taskqueue.data.CreateTaskRequestDecoder;
import org.camunda.tngp.taskqueue.data.TaskInstanceDecoder;
import org.camunda.tngp.taskqueue.data.TaskInstanceRequestDecoder;
import org.camunda.tngp.taskqueue.data.WfDefinitionDecoder;
import org.camunda.tngp.taskqueue.data.WfDefinitionRequestDecoder;
import org.camunda.tngp.taskqueue.data.WfDefinitionRuntimeRequestDecoder;
import org.camunda.tngp.taskqueue.data.WorkflowInstanceRequestDecoder;
import org.camunda.tngp.util.buffer.BufferReader;

import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;

/**
 * An instance of {@link Templates} is stateful and not thread-safe
 */
public class Templates
{

    // TODO: this could even become based on arrays, given that we know the minimal template id and
    // the template ids are ascending (which we could enforce)
    protected static final Int2ObjectHashMap<Template<?>> TEMPLATES = new Int2ObjectHashMap<>();

    // wf runtime
    public static final Template<BpmnActivityEventReader> ACTIVITY_EVENT =
            newTemplate(BpmnActivityEventDecoder.TEMPLATE_ID, BpmnActivityEventReader.class);
    public static final Template<BpmnProcessEventReader> PROCESS_EVENT =
            newTemplate(BpmnProcessEventDecoder.TEMPLATE_ID, BpmnProcessEventReader.class);
    public static final Template<BpmnFlowElementEventReader> FLOW_ELEMENT_EVENT =
            newTemplate(BpmnFlowElementEventDecoder.TEMPLATE_ID, BpmnFlowElementEventReader.class);

    public static final Template<WorkflowInstanceRequestReader> WF_INSTANCE_REQUEST =
            newTemplate(WorkflowInstanceRequestDecoder.TEMPLATE_ID, WorkflowInstanceRequestReader.class);
    public static final Template<ActivityInstanceRequestReader> ACTIVITY_INSTANCE_REQUEST =
            newTemplate(ActivityInstanceRequestDecoder.TEMPLATE_ID, ActivityInstanceRequestReader.class);

    public static final Template<WfDefinitionRuntimeRequestReader> WF_DEFINITION_RUNTIME_REQUEST =
            newTemplate(WfDefinitionRuntimeRequestDecoder.TEMPLATE_ID, WfDefinitionRuntimeRequestReader.class);

    // wf repository
    public static final Template<WfDefinitionReader> WF_DEFINITION =
            newTemplate(WfDefinitionDecoder.TEMPLATE_ID, WfDefinitionReader.class);

    public static final Template<WfDefinitionRequestReader> WF_DEFINITION_REQUEST =
            newTemplate(WfDefinitionRequestDecoder.TEMPLATE_ID, WfDefinitionRequestReader.class);

    // task
    public static final Template<TaskInstanceReader> TASK_INSTANCE =
            newTemplate(TaskInstanceDecoder.TEMPLATE_ID, TaskInstanceReader.class);
    public static final Template<TaskInstanceRequestReader> TASK_INSTANCE_REQUEST =
            newTemplate(TaskInstanceRequestDecoder.TEMPLATE_ID, TaskInstanceRequestReader.class);
    public static final Template<CreateTaskInstanceRequestReader> CREATE_TASK_REQUEST =
            newTemplate(CreateTaskRequestDecoder.TEMPLATE_ID, CreateTaskInstanceRequestReader.class);


    protected static <T extends BufferReader> Template<T> newTemplate(int templateId, Class<T> readerClass)
    {
        if (TEMPLATES.containsKey(templateId))
        {
            throw new RuntimeException("Cannot register two templates with same id");
        }

        final Template<T> template = new Template<>(templateId, readerClass);
        TEMPLATES.put(templateId, template);
        return template;
    }


    protected Map<Template<?>, BufferReader> entryReaders = new HashMap<>();

    public Templates(Template<?>... templates)
    {
        for (Template<?> template : templates)
        {
            this.entryReaders.put(template, template.newReader());
        }
    }

    public static Templates wfRuntimeLogTemplates()
    {
        return new Templates(
                ACTIVITY_EVENT,
                PROCESS_EVENT,
                FLOW_ELEMENT_EVENT,
                WF_INSTANCE_REQUEST,
                ACTIVITY_INSTANCE_REQUEST,
                WF_DEFINITION_RUNTIME_REQUEST);
    }

    public static Templates wfRepositoryLogTemplates()
    {
        return new Templates(
                WF_DEFINITION,
                WF_DEFINITION_REQUEST);
    }

    public static Templates taskQueueLogTemplates()
    {
        return new Templates(
                TASK_INSTANCE,
                TASK_INSTANCE_REQUEST,
                CREATE_TASK_REQUEST);
    }

    public <S extends BufferReader> S getReader(Template<S> template)
    {
        // TODO: do something if template not contained
        return (S) entryReaders.get(template);
    }

    public static <T extends BufferReader> Template<T> getTemplate(int id)
    {
        // TODO: throw exception if template does not exist
        return (Template<T>) TEMPLATES.get(id);
    }
}