package db

import (
	"context"

	"mini-k8ts/internal/models"
)

type TaskRepository interface {
	Create(context.Context, models.Task) (models.Task, error)
	GetTask(context.Context, string) (models.Task, error)
	ListTasks(context.Context) ([]models.Task, error)
	ListPending(context.Context) ([]models.Task, error)
	ListTasksByNodeAndStatus(context.Context, string, models.TaskStatus) ([]models.Task, error)
	ListTasksByJob(context.Context, string) ([]models.Task, error)
	UpdateStatus(context.Context, string, models.TaskStatus, string) error
	UpdateTask(context.Context, models.Task) error
}

type NodeRepository interface {
	Register(context.Context, models.Node) (models.Node, error)
	Heartbeat(context.Context, string, int, int) error
	RecordTaskResult(context.Context, string, models.TaskStatus) error
	ListNodes(context.Context) ([]models.Node, error)
	MarkDownInactive(context.Context) (int64, error)
}

type AllocationRepository interface {
	CreateAllocation(context.Context, models.Allocation) error
	ListByNode(context.Context, string) ([]models.Allocation, error)
	DeleteAllocation(context.Context, string, string) error
	DeleteAllocationsByTask(context.Context, string) error
}

type TaskAllocationWriter interface {
	UpdateStatusAndAllocation(context.Context, string, models.TaskStatus, string) error
}

type JobRepository interface {
	CreateJob(context.Context, models.Job) (models.Job, error)
	GetJob(context.Context, string) (models.Job, error)
	ListJobs(context.Context) ([]models.Job, error)
	UpdateJob(context.Context, models.Job) error
}

type NamespaceRepository interface {
	CreateNamespace(context.Context, models.Namespace) (models.Namespace, error)
	GetNamespace(context.Context, string) (models.Namespace, error)
	ListNamespaces(context.Context) ([]models.Namespace, error)
	UpdateNamespace(context.Context, models.Namespace) error
}

type ServiceRepository interface {
	CreateService(context.Context, models.Service) (models.Service, error)
	GetService(context.Context, string, string) (models.Service, error)
	ListServices(context.Context) ([]models.Service, error)
	UpdateService(context.Context, models.Service) error
}

type SecretRepository interface {
	CreateSecret(context.Context, models.Secret) (models.Secret, error)
	GetSecret(context.Context, string, string) (models.Secret, error)
	ListSecrets(context.Context, string) ([]models.Secret, error)
}
