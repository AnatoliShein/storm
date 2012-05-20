(ns backtype.storm.daemon.task
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm bootstrap])
  (:import [java.util.concurrent ConcurrentLinkedQueue ConcurrentHashMap LinkedBlockingQueue])
  (:import [backtype.storm.hooks ITaskHook])
  (:import [backtype.storm.tuple Tuple])
  (:import [backtype.storm.generated SpoutSpec Bolt StateSpoutSpec])
  (:import [backtype.storm.hooks.info SpoutAckInfo SpoutFailInfo
              EmitInfo BoltFailInfo BoltAckInfo])
  (:require [backtype.storm [tuple :as tuple]]))

(bootstrap)

(defn mk-topology-context-builder [worker topology]
  (let [conf (:conf worker)]
    #(TopologyContext.
      topology
      (:storm-conf worker)
      (:task->component worker)
      (:component->sorted-tasks worker)
      (:storm-id worker)
      (supervisor-storm-resources-path
        (supervisor-stormdist-root conf (:storm-id worker)))
      (worker-pids-root conf (:worker-id worker))
      (int %)
      (:port worker)
      (:task-ids worker)
      )))

(defn system-topology-context [worker tid]
  ((mk-topology-context-builder
    worker
    (:system-topology worker))
   tid))

(defn user-topology-context [worker tid]
  ((mk-topology-context-builder
    worker
    (:topology worker))
   tid))

(defn- get-task-object [^TopologyContext topology component-id]
  (let [spouts (.get_spouts topology)
        bolts (.get_bolts topology)
        state-spouts (.get_state_spouts topology)
        obj (Utils/getSetComponentObject
             (cond
              (contains? spouts component-id) (.get_spout_object ^SpoutSpec (get spouts component-id))
              (contains? bolts component-id) (.get_bolt_object ^Bolt (get bolts component-id))
              (contains? state-spouts component-id) (.get_state_spout_object ^StateSpoutSpec (get state-spouts component-id))
              true (throw-runtime "Could not find " component-id " in " topology)))
        obj (if (instance? ShellComponent obj)
              (if (contains? spouts component-id)
                (ShellSpout. obj)
                (ShellBolt. obj))
              obj )
        obj (if (instance? JavaObject obj)
              (thrift/instantiate-java-object obj)
              obj )]
    obj
    ))

(defn get-context-hooks [^TopologyContext context]
  (.getHooks context))

(defmacro apply-hooks [topology-context method-sym info-form]
  (let [hook-sym (with-meta (gensym "hook") {:tag 'backtype.storm.hooks.ITaskHook})]
    `(let [hooks# (get-context-hooks ~topology-context)]
       (when-not (empty? hooks#)
         (let [info# ~info-form]
           (doseq [~hook-sym hooks#]
             (~method-sym ~hook-sym info#)
             ))))))

(defn send-unanchored [task-data stream values]
  (let [^TopologyContext topology-context (:system-context task-data)
        tasks-fn (:tasks-fn task-data)
        transfer-fn (-> task-data :executor-data :transfer-fn)]
    (doseq [t (tasks-fn stream values)]
      (transfer-fn t
                   (Tuple. topology-context
                           values
                           (.getThisTaskId topology-context)
                           stream)))))

(defn mk-tasks-fn [task-data]
  (let [executor-data (:executor-data task-data)
        component-id (:component-id executor-data)
        ^WorkerTopologyContext worker-context (:worker-context executor-data)
        storm-conf (:storm-conf executor-data)
        emit-sampler (mk-stats-sampler storm-conf)
        stream->component->grouper (:stream->component->grouper executor-data)
        user-context (:user-context task-data)
        executor-stats (:stats executor-data)]
    (fn ([^Integer out-task-id ^String stream ^List values]
          (when (= true (storm-conf TOPOLOGY-DEBUG))
            (log-message "Emitting direct: " out-task-id "; " component-id " " stream " " values))
          (let [target-component (.getComponentId worker-context out-task-id)
                component->grouping (stream->component->grouper stream)
                grouping (get component->grouping target-component)
                out-task-id (if grouping out-task-id)]
            (when (and (not-nil? grouping) (not= :direct grouping))
              (throw (IllegalArgumentException. "Cannot emitDirect to a task expecting a regular grouping")))                          
            (apply-hooks user-context .emit (EmitInfo. values stream [out-task-id]))
            (when (emit-sampler)
              (stats/emitted-tuple! executor-stats stream)
              (if out-task-id
                (stats/transferred-tuples! executor-stats stream 1)))
            (if out-task-id [out-task-id])
            ))
        ([^String stream ^List values]
           (when (= true (storm-conf TOPOLOGY-DEBUG))
             (log-message "Emitting: " component-id " " stream " " values))
           (let [;; TODO: this doesn't seem to be very fast
                 ;; and seems to be the current bottleneck
                 out-tasks (mapcat
                            (fn [[out-component grouper]]
                              (when (= :direct grouper)
                                ;;  TODO: this is wrong, need to check how the stream was declared
                                (throw (IllegalArgumentException. "Cannot do regular emit to direct stream")))
                              (collectify (grouper values)))
                            (stream->component->grouper stream))]
             (apply-hooks user-context .emit (EmitInfo. values stream out-tasks))
             (when (emit-sampler)
               (stats/emitted-tuple! executor-stats stream)
               (stats/transferred-tuples! executor-stats stream (count out-tasks)))
             out-tasks)))
    ))

(defn mk-task-data [executor-data task-id]
  (recursive-map
    :executor-data executor-data
    :task-id task-id
    :system-context (system-topology-context (:worker executor-data) task-id)
    :user-context (user-topology-context (:worker executor-data) task-id)
    :tasks-fn (mk-tasks-fn <>)
    :object (get-task-object (.getRawTopology ^TopologyContext (:system-context <>)) (:component-id executor-data))
    ))


(defn mk-task [executor-data task-id]
  (let [task-data (mk-task-data executor-data task-id)
        storm-conf (:storm-conf executor-data)]
    (doseq [klass (storm-conf TOPOLOGY-AUTO-TASK-HOOKS)]
      (.addTaskHook ^TopologyContext (:user-context task-data) (-> klass Class/forName .newInstance)))
    (send-unanchored task-data SYSTEM-STREAM-ID ["startup"])
    task-data
    ))