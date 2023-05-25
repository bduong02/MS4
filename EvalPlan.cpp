#include "EvalPlan.h"

class Dummy : public DbRelation {
public:
    static Dummy &one() {
        static Dummy d;
        return d;
    }

    Dummy() : DbRelation("dummy", ColumnNames(), ColumnAttributes()) {}

    virtual void create() {};

    virtual void create_if_not_exists() {};

    virtual void drop() {};

    virtual void open() {};

    virtual void close() {};

    virtual Handle insert(const ValueDict *row) { return Handle(); }

    virtual void update(const Handle handle, const ValueDict *new_values) {}

    virtual void del(const Handle handle) {}

    virtual Handles *select() { return nullptr; };

    virtual Handles *select(const ValueDict *where) { return nullptr; }

    virtual Handles *select(Handles *current_selection, const ValueDict *where) { return nullptr; }

    virtual ValueDict *project(Handle handle) { return nullptr; }

    virtual ValueDict *project(Handle handle, const ColumnNames *column_names) { return nullptr; }
};

EvalPlan::EvalPlan(EvalPlan *relation) : type(Select), relation(relation),
                                         projection(nullptr), table(Dummy::one()) {
}

EvalPlan::EvalPlan(ColumnNames *projection, EvalPlan *relation) : type(Project), relation(relation),
                                                                  projection(projection), table(Dummy::one()) {
}

EvalPlan::EvalPlan(DbRelation &table) : type(TableScan), relation(nullptr), projection(nullptr),
                                        table(table) {
}

EvalPlan::~EvalPlan() {
    delete relation;
    delete projection;
}