Add changes here for all PR submitted to the develop branch.

<!-- Please add the `changes` to the following location(feature/bugfix/optimize/test) based on the type of PR -->

### feature:
- [[#4802](https://github.com/seata/seata/pull/4802)] dockerfile support arm64
- [[#4863](https://github.com/seata/seata/pull/4863)] support oracle and postgresql multi primary key
- [[#4649](https://github.com/seata/seata/pull/4649)] seata-server support multiple registry
- [[#4479](https://github.com/seata/seata/pull/4479)] TCC mode supports tcc annotation marked on both interface and implementation class
- [[#4877](https://github.com/seata/seata/pull/4877)] seata client support jdk17
- [[#4468](https://github.com/seata/seata/pull/4968)] support kryo 5.3.0
- [[#4914](https://github.com/seata/seata/pull/4914)] support mysql update join sql


### bugfix:
- [[#4780](https://github.com/seata/seata/pull/4780)] fix can't post TimeoutRollbacked event after a successful timeout rollback
- [[#4954](https://github.com/seata/seata/pull/4954)] fix output expression incorrectly throws npe
- [[#4817](https://github.com/seata/seata/pull/4817)] fix in high version springboot property not Standard
- [[#4838](https://github.com/seata/seata/pull/4838)] fix when use Statement.executeBatch() can not generate undo log
- [[#4533](https://github.com/seata/seata/pull/4533)] fix rollback event repeated and some event status not correct
- [[#4779](https://github.com/seata/seata/pull/4779)] fix and support Apache Dubbo 3
- [[#4912](https://github.com/seata/seata/pull/4912)] fix mysql InsertOnDuplicateUpdate column case is different and cannot be matched
- [[#4543](https://github.com/seata/seata/pull/4543)] fix support Oracle nclob types
- [[#4915](https://github.com/seata/seata/pull/4915)] fix failed to get server recovery properties
- [[#4919](https://github.com/seata/seata/pull/4919)] fix XID port  and  address null:0 before coordinator.init
- [[#4928](https://github.com/seata/seata/pull/4928)] fix rpcContext.getClientRMHolderMap NPE 
- [[#4953](https://github.com/seata/seata/pull/4953)] fix InsertOnDuplicateUpdate bypass modify pk
- [[#4978](https://github.com/seata/seata/pull/4978)] fix kryo support circular reference
- [[#4874](https://github.com/seata/seata/pull/4874)] fix startup failure of Server1.5.2 by using OpenJDK 11 
- [[#5018](https://github.com/seata/seata/pull/5018)] fix loader path in startup scripts
- [[#5004](https://github.com/seata/seata/pull/5004)] fix duplicate image row for update join
- [[#5033](https://github.com/seata/seata/pull/5033)] fix null exception when sql columns is empty for insert on duplicate
- [[#5033](https://github.com/seata/seata/pull/5023)] fix mysql InsertOnDuplicateUpdate insert value type recognize error
- [[#5038](https://github.com/seata/seata/pull/5038)] remove @EnableConfigurationProperties({SagaAsyncThreadPoolProperties.class})
- [[#5050](https://github.com/seata/seata/pull/5050)] fix global session is not change to Committed in saga mode



### optimize:
- [[#4774](https://github.com/seata/seata/pull/4774)] optimize mysql8 dependencies for seataio/seata-server image
- [[#4790](https://github.com/seata/seata/pull/4790)] Add a github action to publish Seata to OSSRH
- [[#4765](https://github.com/seata/seata/pull/4765)] mysql 8.0.29 not should be hold for connection
- [[#4750](https://github.com/seata/seata/pull/4750)] optimize unBranchLock romove xid
- [[#4797](https://github.com/seata/seata/pull/4797)] optimize the github actions
- [[#4800](https://github.com/seata/seata/pull/4800)] Add NOTICE as Apache License V2
- [[#4761](https://github.com/seata/seata/pull/4761)] use hget replace hmget because only one field
- [[#4414](https://github.com/seata/seata/pull/4414)] exclude log4j dependencies
- [[#4836](https://github.com/seata/seata/pull/4836)] optimize BaseTransactionalExecutor#buildLockKey(TableRecords rowsIncludingPK) method more readable
- [[#4865](https://github.com/seata/seata/pull/4865)] fix some security vulnerabilities in GGEditor
- [[#4590](https://github.com/seata/seata/pull/4590)] auto degrade enable to dynamic configure
- [[#4490](https://github.com/seata/seata/pull/4490)] tccfence log table delete by index
- [[#4911](https://github.com/seata/seata/pull/4911)] add license checker workflow
- [[#4917](https://github.com/seata/seata/pull/4917)] upgrade package-lock.json fix vulnerabilities
- [[#4924](https://github.com/seata/seata/pull/4924)] optimize pom dependencies
- [[#4932](https://github.com/seata/seata/pull/4932)] extract the default values for some properties
- [[#4925](https://github.com/seata/seata/pull/4925)] optimize java doc warning
- [[#4921](https://github.com/seata/seata/pull/4921)] fix some vulnerabilities in console and upgrade skywalking-eyes
- [[#4936](https://github.com/seata/seata/pull/4936)] optimize read of storage configuration
- [[#4946](https://github.com/seata/seata/pull/4946)] pass the sqlexception to client when get lock
- [[#4962](https://github.com/seata/seata/pull/4962)] optimize build and fix the base image
- [[#4974](https://github.com/seata/seata/pull/4974)] optimize cancel the limit on the number of globalStatus queries in Redis mode
- [[#4981](https://github.com/seata/seata/pull/4981)] optimize tcc fence record not exists errMessage
- [[#4985](https://github.com/seata/seata/pull/4985)] fix undo_log id repeat
- [[#4995](https://github.com/seata/seata/pull/4995)] fix mysql InsertOnDuplicateUpdate duplicate pk condition in after image query sql
- [[#5047](https://github.com/seata/seata/pull/5047)] remove useless code
- [[#5051](https://github.com/seata/seata/pull/5051)] undo log dirty throw BranchRollbackFailed_Unretriable


### test:
- [[#4794](https://github.com/seata/seata/pull/4794)] try to fix the test `DataSourceProxyTest.getResourceIdTest()`


Thanks to these contributors for their code commits. Please report an unintended omission.

<!-- Please make sure your Github ID is in the list below -->
- [slievrly](https://github.com/slievrly)
- [tuwenlin](https://github.com/tuwenlin)
- [lcmvs](https://github.com/lcmvs)
- [wangliang181230](https://github.com/wangliang181230)
- [a364176773](https://github.com/a364176773)
- [AlexStocks](https://github.com/AlexStocks)
- [liujunlin5168](https://github.com/liujunlin5168)
- [pengten](https://github.com/pengten)
- [liuqiufeng](https://github.com/liuqiufeng)
- [yujianfei1986](https://github.com/yujianfei1986)
- [Bughue](https://github.com/Bughue)
- [AlbumenJ](https://github.com/AlbumenJ)
- [doubleDimple](https://github.com/doubleDimple)
- [jsbxyyx](https://github.com/jsbxyyx)
- [tuwenlin](https://github.com/tuwenlin)
- [CrazyLionLi](https://github.com/JavaLionLi)
- [whxxxxx](https://github.com/whxxxxx)
- [renliangyu857](https://github.com/renliangyu857)
- [neillee95](https://github.com/neillee95)
- [crazy-sheep](https://github.com/crazy-sheep)
- [zhangzq7](https://github.com/zhangzq7)

Also, we receive many valuable issues, questions and advices from our community. Thanks for you all.
