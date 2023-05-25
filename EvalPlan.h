#pragma once

#include "storage_engine.h"

typedef std::pair<DbRelation *, Handles *> EvalPipeline;

class EvalPlan {
    public:
        enum PlanType {
            Project, Select, TableScan
        };
    
    EvalPlan(EvalPlan *relation); // select
    EvalPlan(ColumnNames *projection, EvalPlan *relation); // project
    EvalPlan(DbRelation &table); // TableScan

    virtual ~EvalPlan();
    ValueDicts *evaluate();
    EvalPipeline pipeline();

    protected:
        PlanType type;
        EvalPlan *relation;
        ColumnNames *projection;
        DbRelation &table;
};