ID = 'id'

S = SUB = SUBJECT = 'subject'
SID = '_subject_id'
ST = '_subject_type'
STID = '_subject_type_id'
STX = '_subject_text'

P = PRD = PREDICATE = 'predicate'
PID = '_predicate_id'
PT = '_predicate_type'
PTID = '_predicate_type_id'
PTX = '_predicate_text'

O = OBJ = OBJECT = 'object'
OID = '_object_id'
OT = '_object_type'
OTID = '_object_type_id'
OTX = '_object_text'

MAX_LENGTH = 1000

FIELD_NAMES = [ID,S,P,O]
FIELD_INDEXES = [1,2,3,4]
ID_IDX,S_IDX,P_IDX,O_IDX = FIELD_INDEXES
FIELDS = zip(FIELD_INDEXES, FIELD_NAMES)
FIELD_IDX_TO_NAME = dict(zip(FIELD_INDEXES,FIELD_NAMES))
FIELD_NAME_TO_IDX = dict(zip(FIELD_NAMES,FIELD_INDEXES))

# A predicate describing a triple belonging to a graph.
BELONGSTOGRAPH = '#belongsToGraph'

ANY = '_'
DONT_CARE = '?'

# Global graphs.
REAL = '#real'
CURRENT = '#current'
HYPOTHETICAL = '#hypothetical'
DELETED = '#deleted'