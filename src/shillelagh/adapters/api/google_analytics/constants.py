from shillelagh.fields import Field, Order, String, Integer, Float, Date, Time, DateTime
from shillelagh.filters import Range

# source: https://ga-dev-tools.web.app/dimensions-metrics-explorer/
ALL_DIMENSIONS = [
    #Date
    ("date", Date(filters=[Range], order= Order.NONE, exact=True)),

    #User
    ("userType",String()),
    ("sessionCount", String()),
    ("daysSinceLastSession", String()),
    ("userDefinedValue", String()),
    ("userBucket", String()),

    #Session
    ("sessionDurationBucket", String()),

    #Traffic sources
    ("referralPath", String()),
    ("fullReferrer", String()),
    ("campaign", String()),
    ("source", String()),
    ("medium", String()),
    ("sourceMedium", String()),
    ("keyword", String()),
    ("adContent", String()),
    ("adContent", String()),
    ("hasSocialSourceReferral", String()),
    ("campaignCode", String()),

    #Ad words
    ("adGroup", String()),
    ("adSlot", String()),
    ("adDistributionNetwork", String()),
    ("adMatchType", String()),
    ("adKeywordMatchType", String()),
    ("adMatchedQuery", String()),
    ("adPlacementDomain", String()),
    ("adPlacementUrl", String()),
    ("adFormat", String()),
    ("adTargetingType", String()),
    ("adTargetingOption", String()),
    ("adDisplayUrl", String()),
    ("adDestinationUrl", String()),
    ("adwordsCustomerID", String()),
    ("adwordsCampaignID", String()),
    ("adwordsAdGroupID", String()),
    ("adwordsCreativeID", String()),
    ("adwordsCriteriaID", String()),
    ("adQueryWordCount", String()),
    ("isTrueViewVideoAd", String()),
]


ALL_METRICS = [
    #User
    ("users", Integer()),
    ("newUsers", Integer()),
    ("percentNewSessions", Float()),
    ("1dayUsers", Integer()),
    ("7dayUsers", Integer()),
    ("14dayUsers", Integer()),
    ("28dayUsers", Integer()),
    ("30dayUsers", Integer()),
    ("sessionsPerUser", Float()),

    #Session
    ("sessions", Integer()),
    ("bounces", Integer()),
    ("bounceRate", Float()),
    ("sessionDuration", Time()),
    ("avgSessionDuration", Time()),
    ("uniqueDimensionCombinations", Integer()),
    ("hits", Integer()),

    #Traffic Sources
    ("organicSearches", Integer()),

    #Ad words
    ("impressions", Integer()),
    ("adClicks", Integer()),
    ("adCost", Float()),
    ("CPM", Float()),
    ("CPC", Float()),
    ("CTR", Float()),
    ("costPerTransaction", Float()),
    ("costPerGoalConversion", Float()),
    ("costPerConversion", Float()),
    ("RPC", Float()),
    ("ROAS", Float())
]

SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]