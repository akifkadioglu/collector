# Collector Design v0.1

Collector is a general system for agentic computing, consising of multiple "tiered" abstractions that build on each other from a searchable data store (a Collection) up to a stateful, multi-tenant "agent mesh".

# Foreword

Collector is a project I've been ruminating about over the past several years. Here's what I want to accomplish:

* A hierarchical registry of identity/auth "namespaces" or tenancy units. Something that allows for nested multitenancy, deep and flexible "scoping" of permissions from the root tenancy unit all the way down through intermediate tenancy units to leaves.
* A "unix-like" universal API for *structured* data.
* A hierarchical registry of APIs and data structures.
* A hierarchical index of actual data, API providers, API implementations, tenancy units, etc. in a multitenant distributed computing environment.
* A 9P-like "everything is a file" data topology, ie an almost completely uniform interface for accessing local data via filesystem, or remote data via the network. Ideally, as close to an "isomorphism" between the data exposed by the network and filesystem as possible.
* A distributed system "microkernel" that implements core control plane, data plane, and "config"/platform/system-leel functionality in one compact service. Something that can serve as the basis for a mesh network.
* Every "dataset" comes with all the usual CRUD APIs, plus search
* Every entity within the system is discoverable and introspectable (the whole thing uses reflection)
* Every data type can be mutated/processed via dynamically registered, persistent functions
* No compromises or leaky abstractions between "ephemeral, fungible, dynamically provisioned" and "persistent, irreplaceable, declaratively provisioned"
* Identity, authn, and authz should "just work"
* Compute, disk, network resources under normal workloads should be serverless/dynamically provisioned without the need for tuning.

## Agent Mesh

These goals are mostly constraints inspired by the ways existing distributed systems, mesh networks, and operating systems fail to provide  AI agents with the right structure for truly autonomous work at scale. Here's their "user story":

Agents should be able to be "dropped into" a compute environment knowing only a small set of core APIs, capable of writing programs that serialize/deserialize/process well-defined data structures according to well-defined funtion interfaces, and possessing enough memory or "working state" to craft and execute non-trivial plans or goals, and understand their environment + drive the execution of their goals at massive computational scale, in coordination with others over a network.

## New Kind of Distributed System

Understandably, existing systems fail to deliver these characteristics. Resource provisioning, api integration, novel dataset creation/processing/sharing, and tenancy units are typically treated as immutable at runtime unless a privileged operator or developer is performing some kind of exceptional operation or intervention. Humans provision and configure these and then go do other things, *preferring* friction and out-of-band configuration to ensure that changes only occur under direct, intentional human supervision, because these kinds of changes typically involve some kind of cost or security implication.

Now that LLMs are capable of writing, calling, and interacting with computers at the API-level, we need something more flexible. A human might could want to set guardrails, or budgets, or limits, or allow/disallow specific actions. But they might want their "agents" to be able to provision resources, or autonomously coordinate and collaborate without direct orchestration.

Service meshes are the closest systems we have to this right now, but they're not built for this kind of use case. Although these agents might be entrusted with non-trivial spending and infrastructure decisions, the specific resources and data they might use to accomplish their goals could be completely ephemeral/dynamic and not useful outside the scope of their task. Or, perhaps they generate quite a lot of useful, important data that their human orchestrators would like to keep - but too much for the human to easily review or process themselves without the LLMs' help in organizing and navigating it. Most service meshes are designed around IAC and stateful control planes where identities and services are long-lived, even if their particular instances aren't.

Also, it's rare for existing distributed systems to account for compute resources that are both *stateful and ephemeral*, but potentially also long-lived, existing on a longevity spectrum that may not be well understand ahead of time. But that's what agents need.

## Fully Semantic Computing

That brings me to the last goal of Collector. LLM "agents" are essentially computers capable of understanding and extending themselves, but unlike LISP or LEAN, they do it with shared semantics to humans' natural languages. So, some might worry that computing via agents risks humans losing insight into what their computers are actually doing.

We can reframe that: computing through highly capable agents would become, from the humans' perspective, fully semantic. Why? For an agent to "know thyself" and "actualize" some semantic human request, even if it requires coordinating with other agents or modifying its own environment, there has to be some stable mapping of the human requests' semantics to the environment and agents' shared semantics.

So as agents accomplish tasks that humans have not quite fully automated or solved (which they would have to discover by searching/finding something matching the human client's semantics), they will effectively need to make their shared computing environment *more semantic*, and importantly, *more closely match human semantics* to aid in discoverability and legibility. This is something humans have been doing with domains, products, names for abstractions and tools, jargon and vocabulary. But now tool-creating agents can automate this, and for maximum results we ought to make these new semantics as universally legible as possible.

Every namespace, type, or service that's given a new discoverable/legible name can actually result in computers more closely matching human semantics. That's what collector can be: a way to make computing completely semantic, and **more human**, with the help of our robot friends.

