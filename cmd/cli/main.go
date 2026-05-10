package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"mini-k8ts/internal/models"
	"mini-k8ts/pkg/client"

	"github.com/fatih/color"
	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
)

var (
	schedulerURL string
	authToken    string
	m8sClient    *client.SchedulerClient
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "m8s",
		Short: "mini-k8ts is a distributed container orchestrator",
		Long: `m8s is the command-line interface for interacting with the mini-k8ts control plane.

Use it to inspect cluster state, submit tasks and jobs, manage namespaces, services, and secrets,
and troubleshoot what the scheduler is doing.`,
		Example: strings.TrimSpace(`
  m8s get nodes
  m8s run nginx:alpine --namespace default
  m8s run job redis:7 --replicas 3 --autoscale
  m8s create secret db-creds --namespace default --from-literal username=app --from-literal password=secret
  m8s get service-endpoints frontend --namespace default --session user-42`),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			m8sClient = client.NewSchedulerClientWithToken(schedulerURL, authToken)
		},
		Run: func(cmd *cobra.Command, args []string) {
			displayBanner()
			cmd.Help()
		},
	}

	rootCmd.PersistentFlags().StringVar(&schedulerURL, "url", "http://localhost:8080", "scheduler API URL")
	rootCmd.PersistentFlags().StringVar(&authToken, "token", os.Getenv("M8S_AUTH_TOKEN"), "scheduler API auth token")
	rootCmd.CompletionOptions.DisableDefaultCmd = true
	rootCmd.SilenceUsage = true
	rootCmd.SilenceErrors = true

	// GET nodes/tasks
	getCmd := &cobra.Command{Use: "get", Short: "Display resources"}
	getCmd.AddCommand(getNodesCmd())
	getCmd.AddCommand(getTasksCmd())
	getCmd.AddCommand(getJobsCmd())
	getCmd.AddCommand(getNamespacesCmd())
	getCmd.AddCommand(getSecretsCmd())
	getCmd.AddCommand(getServicesCmd())
	getCmd.AddCommand(getServiceEndpointsCmd())
	getCmd.AddCommand(getInsightsCmd())
	getCmd.AddCommand(getMetricsCmd())
	rootCmd.AddCommand(getCmd)

	// DESCRIBE task/node
	describeCmd := &cobra.Command{Use: "describe", Short: "Show details of a resource"}
	describeCmd.AddCommand(describeTaskCmd())
	describeCmd.AddCommand(describeNodeCmd())
	describeCmd.AddCommand(describeJobCmd())
	describeCmd.AddCommand(describeSecretCmd())
	rootCmd.AddCommand(describeCmd)

	// TOP command
	rootCmd.AddCommand(topCmd())

	// RUN task
	rootCmd.AddCommand(runCmd())

	// CREATE namespace/service/secret
	createCmd := &cobra.Command{Use: "create", Short: "Create resources"}
	createCmd.AddCommand(createNamespaceCmd())
	createCmd.AddCommand(createSecretCmd())
	createCmd.AddCommand(createServiceCmd())
	rootCmd.AddCommand(createCmd)

	// LOGS task
	rootCmd.AddCommand(logsCmd())

	// HELP command
	rootCmd.AddCommand(&cobra.Command{
		Use:   "help",
		Short: "Display help information",
		Run: func(cmd *cobra.Command, args []string) {
			rootCmd.Help()
		},
	})

	// COMPLETION command
	rootCmd.AddCommand(completionCmd(rootCmd))

	if err := rootCmd.Execute(); err != nil {
		color.Red("Error: %v", err)
		fmt.Println("Run 'm8s help' to explore commands.")
		os.Exit(1)
	}
}

func completionCmd(root *cobra.Command) *cobra.Command {
	cmd := &cobra.Command{
		Use:       "completion [bash|zsh|fish|powershell]",
		Short:     "Generate shell completion scripts",
		Args:      cobra.ExactArgs(1),
		ValidArgs: []string{"bash", "zsh", "fish", "powershell"},
		RunE: func(cmd *cobra.Command, args []string) error {
			shell := strings.ToLower(strings.TrimSpace(args[0]))
			out := cmd.OutOrStdout()
			switch shell {
			case "bash":
				return root.GenBashCompletion(out)
			case "zsh":
				return root.GenZshCompletion(out)
			case "fish":
				return root.GenFishCompletion(out, true)
			case "powershell":
				return root.GenPowerShellCompletionWithDesc(out)
			default:
				return fmt.Errorf("unsupported shell: %s", shell)
			}
		},
	}

	return cmd
}

func displayBanner() {
	displayBannerToWriter(os.Stdout)
}

func displayBannerToWriter(w io.Writer) {
	banner := `
███╗   ███╗██╗███╗   ██╗██╗     ██╗  ██╗  █████╗  ████████╗███████╗
████╗ ████║██║████╗  ██║██║     ██║ ██╔╝ ██╔══██╗ ╚══██╔══╝██╔════╝
██╔████╔██║██║██╔██╗ ██║██║     █████╔╝  ╚█████╔╝    ██║   ███████╗
██║╚██╔╝██║██║██║╚██╗██║██║     ██╔═██╗  ██╔══██╗    ██║   ╚════██║
██║ ╚═╝ ██║██║██║ ╚████║██║     ██║  ██╗ ╚█████╔╝    ██║   ███████║
╚═╝     ╚═╝╚═╝╚═╝  ╚═══╝╚═╝     ╚═╝  ╚═╝  ╚════╝     ╚═╝   ╚══════╝`

	green := color.New(color.FgHiGreen, color.Bold)
	green.Fprintln(w, banner)
	fmt.Fprintln(w)
}

func getNodesCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "nodes",
		Short: "List all nodes in the cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			nodes, _, err := m8sClient.GetClusterState(context.Background())
			if err != nil {
				return err
			}

			fmt.Printf("%s %s %s %s %s\n",
				padRight("NODE ID", 15),
				padRight("STATUS", 10),
				padRight("CPU (USED/TOTAL)", 18),
				padRight("MEM (USED/TOTAL)", 18),
				"RELIABILITY")

			for _, n := range nodes {
				statusColor := color.New(color.FgGreen).SprintFunc()
				if n.Status != "READY" {
					statusColor = color.New(color.FgRed).SprintFunc()
				}

				statusStr := statusColor(n.Status)

				fmt.Printf("%s %s %s %s %.2f\n",
					padRight(n.ID, 15),
					padRight(statusStr, 10),
					padRight(fmt.Sprintf("%d/%d", n.UsedCPU, n.TotalCPU), 18),
					padRight(fmt.Sprintf("%d/%d MB", n.UsedMemory, n.TotalMemory), 18),
					n.Reliability,
				)
			}
			return nil
		},
	}
}

func getTasksCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "tasks",
		Short: "List all tasks in the cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			_, tasks, err := m8sClient.GetClusterState(context.Background())
			if err != nil {
				return err
			}

			fmt.Printf("%s %s %s %s %s %s\n",
				padRight("TASK ID", 25),
				padRight("IMAGE", 20),
				padRight("STATUS", 10),
				padRight("NODE", 15),
				padRight("ATTEMPTS", 10),
				"DETAIL")

			for _, t := range tasks {
				statusColor := color.New(color.FgCyan).SprintFunc()
				if t.Status == "RUNNING" {
					statusColor = color.New(color.FgGreen).SprintFunc()
				} else if t.Status == "FAILED" {
					statusColor = color.New(color.FgRed).SprintFunc()
				}

				nodeID := t.NodeID
				if nodeID == "" {
					nodeID = "<none>"
				}

				fmt.Printf("%s %s %s %s %s %s\n",
					padRight(t.ID, 25),
					padRight(t.Image, 20),
					padRight(statusColor(t.Status), 10),
					padRight(nodeID, 15),
					padRight(fmt.Sprintf("%d/%d", t.Attempts, t.MaxAttempts), 10),
					truncateText(taskSummaryDetail(t), 44),
				)
			}
			return nil
		},
	}
}

func getJobsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "jobs",
		Short: "List all jobs in the cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			jobs, err := m8sClient.GetJobs(context.Background())
			if err != nil {
				return err
			}

			fmt.Printf("%s %s %s %s %s %s\n",
				padRight("JOB ID", 20),
				padRight("IMAGE", 20),
				padRight("STATUS", 12),
				padRight("REPLICAS", 10),
				padRight("PRIORITY", 10),
				"CPU/MEM")

			for _, job := range jobs {
				statusColor := color.New(color.FgCyan).SprintFunc()
				if job.Status == "RUNNING" {
					statusColor = color.New(color.FgGreen).SprintFunc()
				} else if job.Status == "FAILED" {
					statusColor = color.New(color.FgRed).SprintFunc()
				}

				fmt.Printf("%s %s %s %s %s %s\n",
					padRight(job.ID, 20),
					padRight(job.Image, 20),
					padRight(statusColor(job.Status), 12),
					padRight(strconv.Itoa(job.Replicas), 10),
					padRight(strconv.Itoa(job.Priority), 10),
					fmt.Sprintf("%d/%dMB", job.CPU, job.Memory),
				)
			}
			return nil
		},
	}
}

func getNamespacesCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "namespaces",
		Short: "List all namespaces",
		RunE: func(cmd *cobra.Command, args []string) error {
			namespaces, err := m8sClient.GetNamespaces(context.Background())
			if err != nil {
				return err
			}

			fmt.Printf("%s %s %s %s\n",
				padRight("NAME", 20),
				padRight("CPU QUOTA", 12),
				padRight("MEM QUOTA", 12),
				"TASK QUOTA")

			for _, ns := range namespaces {
				fmt.Printf("%s %s %s %d\n",
					padRight(ns.Name, 20),
					padRight(strconv.Itoa(ns.CPUQuota), 12),
					padRight(fmt.Sprintf("%dMB", ns.MemoryQuota), 12),
					ns.TaskQuota,
				)
			}
			return nil
		},
	}
}

func getSecretsCmd() *cobra.Command {
	var namespace string

	cmd := &cobra.Command{
		Use:   "secrets",
		Short: "List secrets in a namespace",
		Example: strings.TrimSpace(`
  m8s get secrets
  m8s get secrets --namespace payments`),
		RunE: func(cmd *cobra.Command, args []string) error {
			secrets, err := m8sClient.GetSecrets(context.Background(), namespace)
			if err != nil {
				return err
			}

			fmt.Printf("%s %s %s\n",
				padRight("NAME", 24),
				padRight("NAMESPACE", 14),
				"KEYS")

			for _, secret := range secrets {
				keys := make([]string, 0, len(secret.Data))
				for key := range secret.Data {
					keys = append(keys, key)
				}
				sort.Strings(keys)
				fmt.Printf("%s %s %s\n",
					padRight(secret.Name, 24),
					padRight(secret.Namespace, 14),
					strings.Join(keys, ","),
				)
			}
			if len(secrets) == 0 {
				color.Yellow("No secrets found in namespace %q.", namespace)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&namespace, "namespace", "default", "Namespace to inspect")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

func getServicesCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "services",
		Short: "List all services",
		RunE: func(cmd *cobra.Command, args []string) error {
			services, err := m8sClient.GetServices(context.Background())
			if err != nil {
				return err
			}

			fmt.Printf("%s %s %s %s\n",
				padRight("NAME", 20),
				padRight("NAMESPACE", 12),
				padRight("JOB ID", 20),
				"PORT")

			for _, svc := range services {
				fmt.Printf("%s %s %s %d\n",
					padRight(svc.Name, 20),
					padRight(svc.Namespace, 12),
					padRight(svc.SelectorJobID, 20),
					svc.Port,
				)
			}
			return nil
		},
	}
}

func getInsightsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "insights",
		Short: "Show cluster insights",
		RunE: func(cmd *cobra.Command, args []string) error {
			insights, err := m8sClient.GetInsights(context.Background())
			if err != nil {
				return err
			}
			if len(insights) == 0 {
				color.Green("No insights to report.")
				return nil
			}
			for _, insight := range insights {
				fmt.Printf("[%s] %s\n", strings.ToUpper(insight.Severity), insight.Message)
				if strings.TrimSpace(insight.Suggestion) != "" {
					fmt.Printf("  Suggestion: %s\n", insight.Suggestion)
				}
			}
			return nil
		},
	}
}

func getServiceEndpointsCmd() *cobra.Command {
	var namespace string
	var session string

	cmd := &cobra.Command{
		Use:   "service-endpoints <serviceName>",
		Short: "Show service endpoints and selected target",
		Example: strings.TrimSpace(`
  m8s get service-endpoints frontend --namespace default
  m8s get service-endpoints frontend --namespace default --session user-42`),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			payload, err := m8sClient.GetServiceEndpoints(context.Background(), namespace, name, session)
			if err != nil {
				return err
			}

			fmt.Printf("Service: %s/%s\n", payload.Service.Namespace, payload.Service.Name)
			if len(payload.Selected) > 0 {
				fmt.Printf("Selected: task=%s node=%s\n", payload.Selected["task_id"], payload.Selected["node_id"])
			} else {
				fmt.Printf("Selected: <none>\n")
			}
			if len(payload.Summary) > 0 {
				fmt.Printf("Summary: healthy=%v degraded=%v unhealthy=%v eligible=%v\n",
					payload.Summary["healthy_endpoints"],
					payload.Summary["degraded_endpoints"],
					payload.Summary["unhealthy_endpoints"],
					payload.Summary["eligible_endpoints"],
				)
			}
			fmt.Printf("Endpoints (%d):\n", len(payload.Endpoints))
			for _, ep := range payload.Endpoints {
				fmt.Printf("- task=%v node=%v health=%v reason=%v\n", ep["task_id"], ep["node_id"], ep["health"], ep["reason"])
			}
			if strings.TrimSpace(session) != "" {
				fmt.Printf("Session key: %s\n", session)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&namespace, "namespace", "default", "Namespace for the service")
	cmd.Flags().StringVar(&session, "session", "", "Sticky session key for stable backend selection")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)
	cmd.ValidArgsFunction = completeServiceNames

	return cmd
}

func runCmd() *cobra.Command {
	var cpu, mem, priority int
	var command []string
	var namespace string
	var secretRefs []string
	var volumes []string

	run := &cobra.Command{
		Use:   "run [image]",
		Short: "Run a new task in the cluster (interactive if no image provided)",
		RunE: func(cmd *cobra.Command, args []string) error {
			var image string
			if len(args) > 0 {
				image = args[0]
			} else {
				// Interactive mode
				imagePrompt := promptui.Prompt{
					Label:   "Container Image (e.g., nginx:alpine)",
					Default: "",
				}
				var err error
				image, err = imagePrompt.Run()
				if err != nil {
					return err
				}
				if image == "" {
					image = "nginx:alpine"
				}

				priorityPrompt := promptui.Select{
					Label: "Select Priority",
					Items: []string{"Low (1)", "Medium (5)", "High (10)"},
				}
				idx, _, err := priorityPrompt.Run()
				if err != nil {
					return err
				}
				switch idx {
				case 0:
					priority = 1
				case 1:
					priority = 5
				case 2:
					priority = 10
				}

				cpuPrompt := promptui.Prompt{
					Label:   "CPU Cores (default: 1)",
					Default: "",
					Validate: func(input string) error {
						if input == "" {
							return nil
						}
						_, err := strconv.Atoi(input)
						return err
					},
				}
				cpuStr, _ := cpuPrompt.Run()
				if cpuStr == "" {
					cpu = 1
				} else {
					cpu, _ = strconv.Atoi(cpuStr)
				}

				memPrompt := promptui.Prompt{
					Label:   "Memory in MB (default: 128)",
					Default: "",
					Validate: func(input string) error {
						if input == "" {
							return nil
						}
						_, err := strconv.Atoi(input)
						return err
					},
				}
				memStr, _ := memPrompt.Run()
				if memStr == "" {
					mem = 128
				} else {
					mem, _ = strconv.Atoi(memStr)
				}
			}

			task := models.Task{
				Namespace: namespace,
				Image:     image,
				CPU:       cpu,
				Memory:    mem,
				Priority:  priority,
				Command:   command,
			}
			parsedSecretRefs, err := parseSecretRefs(secretRefs)
			if err != nil {
				return err
			}
			parsedVolumes, err := parseVolumeMounts(volumes)
			if err != nil {
				return err
			}
			task.SecretRefs = parsedSecretRefs
			task.Volumes = parsedVolumes

			color.Yellow("Submitting task: %s (Priority: %d, CPU: %d, Mem: %dMB)...", image, priority, cpu, mem)
			taskID, err := m8sClient.SubmitTask(context.Background(), task)
			if err != nil {
				return err
			}

			color.Green("🚀 Task submitted successfully!")
			fmt.Printf("Task ID: %s\n", taskID)
			fmt.Printf("Use './bin/m8s describe task %s' for details.\n", taskID)
			return nil
		},
	}

	run.Flags().IntVar(&cpu, "cpu", 1, "CPU cores required")
	run.Flags().IntVar(&mem, "mem", 128, "Memory in MB required")
	run.Flags().IntVar(&priority, "priority", 1, "Task priority (higher is better)")
	run.Flags().StringSliceVar(&command, "command", []string{}, "Command to run (comma separated)")
	run.Flags().StringVar(&namespace, "namespace", "default", "Namespace for the task")
	run.Flags().StringArrayVar(&secretRefs, "secret", nil, "Secret ref as name:key:ENV_VAR (repeatable)")
	run.Flags().StringArrayVar(&volumes, "volume", nil, "Volume mount as source:target[:ro] (repeatable)")
	_ = run.RegisterFlagCompletionFunc("namespace", completeNamespaces)
	run.AddCommand(runJobCmd())

	return run
}

func runJobCmd() *cobra.Command {
	var cpu, mem, priority, replicas int
	var command []string
	var namespace string
	var autoscaleEnabled bool
	var minReplicas int
	var maxReplicas int
	var scaleDownCPU int
	var scaleUpPending int
	var scaleUpStep int
	var secretRefs []string
	var volumes []string

	cmd := &cobra.Command{
		Use:   "job [image]",
		Short: "Run a new job in the cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			var image string
			if len(args) > 0 {
				image = args[0]
			} else {
				imagePrompt := promptui.Prompt{
					Label:   "Job Image (e.g., nginx:alpine)",
					Default: "",
				}
				var err error
				image, err = imagePrompt.Run()
				if err != nil {
					return err
				}
				if image == "" {
					image = "nginx:alpine"
				}
			}

			autoscale := models.Autoscale{
				Enabled:        autoscaleEnabled,
				MinReplicas:    minReplicas,
				MaxReplicas:    maxReplicas,
				ScaleDownCPU:   scaleDownCPU,
				ScaleUpPending: scaleUpPending,
				ScaleUpStep:    scaleUpStep,
			}

			job := models.Job{
				Namespace: namespace,
				Image:     image,
				Command:   command,
				Replicas:  replicas,
				CPU:       cpu,
				Memory:    mem,
				Priority:  priority,
				Autoscale: autoscale,
			}
			parsedSecretRefs, err := parseSecretRefs(secretRefs)
			if err != nil {
				return err
			}
			parsedVolumes, err := parseVolumeMounts(volumes)
			if err != nil {
				return err
			}
			job.SecretRefs = parsedSecretRefs
			job.Volumes = parsedVolumes

			color.Yellow("Submitting job: %s (Replicas: %d, CPU: %d, Mem: %dMB)...", image, replicas, cpu, mem)
			jobID, err := m8sClient.CreateJob(context.Background(), job)
			if err != nil {
				return err
			}

			color.Green("🚀 Job submitted successfully!")
			fmt.Printf("Job ID: %s\n", jobID)
			fmt.Printf("Use './bin/m8s describe job %s' for details.\n", jobID)
			return nil
		},
	}

	cmd.Flags().IntVar(&replicas, "replicas", 1, "Number of replicas to run")
	cmd.Flags().IntVar(&cpu, "cpu", 1, "CPU cores required")
	cmd.Flags().IntVar(&mem, "mem", 128, "Memory in MB required")
	cmd.Flags().IntVar(&priority, "priority", 1, "Job priority (higher is better)")
	cmd.Flags().StringSliceVar(&command, "command", []string{}, "Command to run (comma separated)")
	cmd.Flags().StringVar(&namespace, "namespace", "default", "Namespace for the job")
	cmd.Flags().BoolVar(&autoscaleEnabled, "autoscale", false, "Enable autoscaling")
	cmd.Flags().IntVar(&minReplicas, "min-replicas", 1, "Minimum replicas when autoscaling")
	cmd.Flags().IntVar(&maxReplicas, "max-replicas", 10, "Maximum replicas when autoscaling")
	cmd.Flags().IntVar(&scaleDownCPU, "scale-down-cpu", 30, "Scale down when cluster CPU usage <= this percentage")
	cmd.Flags().IntVar(&scaleUpPending, "scale-up-pending", 1, "Scale up when pending tasks reach this count")
	cmd.Flags().IntVar(&scaleUpStep, "scale-up-step", 1, "Replica increment when scaling up")
	cmd.Flags().StringArrayVar(&secretRefs, "secret", nil, "Secret ref as name:key:ENV_VAR (repeatable)")
	cmd.Flags().StringArrayVar(&volumes, "volume", nil, "Volume mount as source:target[:ro] (repeatable)")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

func createNamespaceCmd() *cobra.Command {
	var cpuQuota int
	var memQuota int
	var taskQuota int

	cmd := &cobra.Command{
		Use:   "namespace <name>",
		Short: "Create a namespace",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ns := models.Namespace{
				Name:        args[0],
				CPUQuota:    cpuQuota,
				MemoryQuota: memQuota,
				TaskQuota:   taskQuota,
			}
			created, err := m8sClient.CreateNamespace(context.Background(), ns)
			if err != nil {
				return err
			}
			color.Green("Namespace created: %s", created.Name)
			return nil
		},
	}

	cmd.Flags().IntVar(&cpuQuota, "cpu-quota", 0, "CPU quota for the namespace")
	cmd.Flags().IntVar(&memQuota, "mem-quota", 0, "Memory quota (MB) for the namespace")
	cmd.Flags().IntVar(&taskQuota, "task-quota", 0, "Task quota for the namespace")
	cmd.ValidArgsFunction = completeNamespaceNames

	return cmd
}

func createSecretCmd() *cobra.Command {
	var namespace string
	var values []string

	cmd := &cobra.Command{
		Use:   "secret <name>",
		Short: "Create or update a namespaced secret",
		Example: strings.TrimSpace(`
  m8s create secret db-creds --namespace default --from-literal username=app --from-literal password=s3cr3t
  m8s create secret api-key --from-literal api_key=abcd1234`),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			data := map[string]string{}
			for _, pair := range values {
				key, value, ok := strings.Cut(pair, "=")
				if !ok || strings.TrimSpace(key) == "" {
					return fmt.Errorf("invalid --from-literal value %q, expected key=value", pair)
				}
				data[strings.TrimSpace(key)] = value
			}
			if len(data) == 0 {
				return fmt.Errorf("at least one --from-literal key=value is required")
			}

			created, err := m8sClient.CreateSecret(context.Background(), models.Secret{
				Name:      args[0],
				Namespace: namespace,
				Data:      data,
			})
			if err != nil {
				return err
			}

			color.Green("Secret stored: %s/%s", created.Namespace, created.Name)
			fmt.Printf("Keys: %s\n", strings.Join(sortedKeys(created.Data), ", "))
			return nil
		},
	}

	cmd.Flags().StringVar(&namespace, "namespace", "default", "Namespace for the secret")
	cmd.Flags().StringArrayVar(&values, "from-literal", nil, "Secret value in key=value form (repeatable)")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

func createServiceCmd() *cobra.Command {
	var namespace string
	var jobID string
	var port int

	cmd := &cobra.Command{
		Use:   "service <name>",
		Short: "Create a service",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(jobID) == "" {
				return fmt.Errorf("--job-id is required")
			}
			svc := models.Service{
				Name:          args[0],
				Namespace:     namespace,
				SelectorJobID: jobID,
				Port:          port,
			}
			created, err := m8sClient.CreateService(context.Background(), svc)
			if err != nil {
				return err
			}
			color.Green("Service created: %s/%s", created.Namespace, created.Name)
			return nil
		},
	}

	cmd.Flags().StringVar(&namespace, "namespace", "default", "Namespace for the service")
	cmd.Flags().StringVar(&jobID, "job-id", "", "Job ID selector for the service")
	cmd.Flags().IntVar(&port, "port", 0, "Service port")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)
	_ = cmd.RegisterFlagCompletionFunc("job-id", completeJobIDs)
	cmd.ValidArgsFunction = completeServiceNames

	return cmd
}

func describeSecretCmd() *cobra.Command {
	var namespace string

	cmd := &cobra.Command{
		Use:   "secret <name>",
		Short: "Describe a secret without printing values",
		Example: strings.TrimSpace(`
  m8s describe secret db-creds
  m8s describe secret db-creds --namespace payments`),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			secret, err := m8sClient.GetSecret(context.Background(), namespace, args[0])
			if err != nil {
				return err
			}

			color.New(color.FgYellow, color.Bold).Printf("Secret: %s/%s\n", secret.Namespace, secret.Name)
			fmt.Printf("  Keys:      %s\n", strings.Join(sortedKeys(secret.Data), ", "))
			fmt.Printf("  Created:   %s\n", secret.CreatedAt.Format(time.RFC1123))
			fmt.Printf("  Updated:   %s\n", secret.UpdatedAt.Format(time.RFC1123))
			fmt.Printf("  Values:    hidden\n")
			return nil
		},
	}

	cmd.Flags().StringVar(&namespace, "namespace", "default", "Namespace for the secret")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

func describeTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "task <taskID>",
		Short: "Describe a task",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			taskID := args[0]
			task, err := m8sClient.GetTask(context.Background(), taskID)
			if err != nil {
				return err
			}

			color.New(color.FgCyan, color.Bold).Printf("Task: %s\n", task.ID)
			fmt.Printf("  Image:      %s\n", task.Image)
			fmt.Printf("  Status:     %s\n", task.Status)
			fmt.Printf("  Priority:   %d\n", task.Priority)
			fmt.Printf("  Node:       %s\n", task.NodeID)
			fmt.Printf("  Resources:  CPU: %d, Memory: %dMB\n", task.CPU, task.Memory)
			fmt.Printf("  Attempts:   %d/%d\n", task.Attempts, task.MaxAttempts)
			fmt.Printf("  Created:    %s\n", task.CreatedAt.Format(time.RFC1123))
			if detail := taskSummaryDetail(task); detail != "" {
				fmt.Printf("  Detail:     %s\n", detail)
			}
			if task.LastError != "" {
				color.Red("  Last Error: %s\n", task.LastError)
			}
			return nil
		},
	}

	cmd.ValidArgsFunction = completeTaskIDs
	return cmd
}

func describeJobCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "job <jobID>",
		Short: "Describe a job",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			jobID := args[0]
			jobs, err := m8sClient.GetJobs(context.Background())
			if err != nil {
				return err
			}
			for _, job := range jobs {
				if job.ID != jobID {
					continue
				}
				color.New(color.FgMagenta, color.Bold).Printf("Job: %s\n", job.ID)
				fmt.Printf("  Image:     %s\n", job.Image)
				fmt.Printf("  Status:    %s\n", job.Status)
				fmt.Printf("  Replicas:  %d\n", job.Replicas)
				fmt.Printf("  Priority:  %d\n", job.Priority)
				fmt.Printf("  Resources: CPU: %d, Memory: %dMB\n", job.CPU, job.Memory)
				fmt.Printf("  Created:   %s\n", job.CreatedAt.Format(time.RFC1123))
				return nil
			}
			return fmt.Errorf("job %s not found", jobID)
		},
	}

	cmd.ValidArgsFunction = completeJobIDs
	return cmd
}

func describeNodeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "node <nodeID>",
		Short: "Describe a node",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			nodeID := args[0]
			node, err := m8sClient.GetNode(context.Background(), nodeID)
			if err != nil {
				return err
			}

			color.New(color.FgGreen, color.Bold).Printf("Node: %s\n", node.ID)
			fmt.Printf("  Status:     %s\n", node.Status)
			fmt.Printf("  Resources:  CPU: %d/%d, Memory: %d/%d MB\n", node.UsedCPU, node.TotalCPU, node.UsedMemory, node.TotalMemory)
			fmt.Printf("  Reliability: %.2f\n", node.Reliability)
			fmt.Printf("  Last Seen:   %s\n", node.LastSeen.Format(time.RFC1123))
			return nil
		},
	}

	cmd.ValidArgsFunction = completeNodeIDs
	return cmd
}

func taskSummaryDetail(task models.Task) string {
	detail := strings.TrimSpace(task.StatusDetail)
	if detail != "" {
		return detail
	}
	return strings.TrimSpace(task.LastError)
}

func truncateText(value string, max int) string {
	if max <= 0 || len(value) <= max {
		return value
	}
	if max <= 3 {
		return value[:max]
	}
	return value[:max-3] + "..."
}

func getMetricsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "metrics",
		Short: "Display cluster-wide metrics",
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := m8sClient.GetMetrics(context.Background())
			if err != nil {
				return err
			}

			color.New(color.FgHiMagenta, color.Bold).Println("Cluster Metrics Snapshot")
			fmt.Println("-------------------------")
			fmt.Printf("Total Tasks:        %d\n", s.TotalTasks)
			fmt.Printf("Failed Tasks:       %d\n", s.FailedTasks)
			fmt.Printf("Success Rate:       %.1f%%\n", s.SuccessRate*100)
			fmt.Printf("Failure Rate:       %.1f%%\n", s.FailureRate*100)
			fmt.Printf("Avg Latency:        %.2fms\n", s.AverageLatency)
			fmt.Println()
			fmt.Printf("CPU Utilization:    %.1f%%\n", s.CPUUtilization*100)
			fmt.Printf("Mem Utilization:    %.1f%%\n", s.MemoryUtilization*100)
			fmt.Printf("Node Reliability:   %.2f\n", s.NodeReliability)
			return nil
		},
	}
}

func topCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "top",
		Short: "Interactive cluster monitor",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Handle Ctrl+C for cleanup
			sigChan := make(chan os.Signal, 1)
			signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

			// Cleanup on exit
			defer func() {
				fmt.Print("\033[?25h") // Show cursor
			}()

			fmt.Print("\033[?25l") // Hide cursor
			fmt.Print("\033[2J")   // Initial clear

			go func() {
				<-sigChan
				fmt.Print("\033[?25h") // Show cursor
				os.Exit(0)
			}()

			for {
				nodes, tasks, err := m8sClient.GetClusterState(context.Background())
				if err != nil {
					color.Red("\rError fetching state: %v", err)
					time.Sleep(2 * time.Second)
					continue
				}

				var buf bytes.Buffer

				// Build the entire screen content in a buffer
				displayBannerToWriter(&buf)

				fmt.Fprintln(&buf, color.New(color.FgHiCyan, color.Bold).Sprint(" CLUSTER MONITOR (Ctrl+C to exit)"))
				fmt.Fprintf(&buf, " Time: %s\n\n", time.Now().Format(time.RFC1123))

				// Nodes Table with manual padding for techie alignment
				fmt.Fprintln(&buf, color.New(color.FgHiGreen, color.Underline).Sprint("NODES"))
				fmt.Fprintf(&buf, "%s %s %s %s %s\n",
					padRight("ID", 15),
					padRight("STATUS", 10),
					padRight("CPU", 15),
					padRight("MEM", 15),
					padRight("RELIABILITY", 10))
				for _, n := range nodes {
					statusStr := color.New(color.FgGreen).Sprint(n.Status)
					if n.Status != "READY" {
						statusStr = color.New(color.FgRed).Sprint(n.Status)
					}

					fmt.Fprintf(&buf, "%s %s %s %s %.2f\n",
						padRight(n.ID, 15),
						padRight(statusStr, 10),
						padRight(fmt.Sprintf("%d/%d", n.UsedCPU, n.TotalCPU), 15),
						padRight(fmt.Sprintf("%d/%dMB", n.UsedMemory, n.TotalMemory), 15),
						n.Reliability,
					)
				}
				fmt.Fprintln(&buf)

				// Tasks Table (Last 10) with manual padding
				fmt.Fprintln(&buf, color.New(color.FgHiYellow, color.Underline).Sprint("TASKS (TOP 10)"))
				fmt.Fprintf(&buf, "%s %s %s %s %s\n",
					padRight("ID", 25),
					padRight("IMAGE", 15),
					padRight("STATUS", 15),
					padRight("NODE", 15),
					padRight("PRIORITY", 10))

				count := 0
				for i := len(tasks) - 1; i >= 0 && count < 10; i-- {
					t := tasks[i]
					statusStr := color.New(color.FgCyan).Sprint(t.Status)
					if t.Status == "RUNNING" {
						statusStr = color.New(color.FgGreen).Sprint(t.Status)
					} else if t.Status == "FAILED" {
						statusStr = color.New(color.FgRed).Sprint(t.Status)
					}

					fmt.Fprintf(&buf, "%s %s %s %s %d\n",
						padRight(t.ID, 25),
						padRight(t.Image, 15),
						padRight(statusStr, 15),
						padRight(t.NodeID, 15),
						t.Priority,
					)
					count++
				}

				// Move to top-left and print the entire buffer
				fmt.Print("\033[H")
				fmt.Print(buf.String())

				// Clear any remaining lines from previous output
				fmt.Print("\033[J")

				time.Sleep(2 * time.Second)
			}
		},
	}
}

func logsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logs <taskID>",
		Short: "Fetch logs for a task",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			taskID := args[0]
			stream, err := m8sClient.GetLogs(context.Background(), taskID)
			if err != nil {
				return err
			}
			defer stream.Close()

			color.Yellow("--- Streaming logs for %s ---", taskID)
			io.Copy(os.Stdout, stream)
			return nil
		},
	}

	cmd.ValidArgsFunction = completeTaskIDs
	return cmd
}

func completionClient(cmd *cobra.Command) *client.SchedulerClient {
	urlValue, err := cmd.Root().PersistentFlags().GetString("url")
	if err != nil || strings.TrimSpace(urlValue) == "" {
		urlValue = "http://localhost:8080"
	}
	return client.NewSchedulerClient(urlValue)
}

func completeTaskIDs(cmd *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	suggestions, err := filterCompletions(toComplete, func() ([]string, error) {
		_, tasks, err := completionClient(cmd).GetClusterState(context.Background())
		if err != nil {
			return nil, err
		}

		items := make([]string, 0, len(tasks))
		for _, task := range tasks {
			items = append(items, task.ID)
		}
		return items, nil
	})
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	return suggestions, cobra.ShellCompDirectiveNoFileComp
}

func completeJobIDs(cmd *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	suggestions, err := filterCompletions(toComplete, func() ([]string, error) {
		jobs, err := completionClient(cmd).GetJobs(context.Background())
		if err != nil {
			return nil, err
		}

		items := make([]string, 0, len(jobs))
		for _, job := range jobs {
			items = append(items, job.ID)
		}
		return items, nil
	})
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	return suggestions, cobra.ShellCompDirectiveNoFileComp
}

func completeNodeIDs(cmd *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	suggestions, err := filterCompletions(toComplete, func() ([]string, error) {
		nodes, _, err := completionClient(cmd).GetClusterState(context.Background())
		if err != nil {
			return nil, err
		}

		items := make([]string, 0, len(nodes))
		for _, node := range nodes {
			items = append(items, node.ID)
		}
		return items, nil
	})
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	return suggestions, cobra.ShellCompDirectiveNoFileComp
}

func completeNamespaces(cmd *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	suggestions, err := namespaceSuggestions(cmd, toComplete)
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	return suggestions, cobra.ShellCompDirectiveNoFileComp
}

func completeNamespaceNames(cmd *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	suggestions, err := namespaceSuggestions(cmd, toComplete)
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	return suggestions, cobra.ShellCompDirectiveNoFileComp
}

func completeServiceNames(cmd *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	suggestions, err := filterCompletions(toComplete, func() ([]string, error) {
		services, err := completionClient(cmd).GetServices(context.Background())
		if err != nil {
			return nil, err
		}

		items := make([]string, 0, len(services))
		for _, service := range services {
			items = append(items, service.Name)
		}
		return items, nil
	})
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	return suggestions, cobra.ShellCompDirectiveNoFileComp
}

func namespaceSuggestions(cmd *cobra.Command, toComplete string) ([]string, error) {
	return filterCompletions(toComplete, func() ([]string, error) {
		namespaces, err := completionClient(cmd).GetNamespaces(context.Background())
		if err != nil {
			return nil, err
		}

		items := make([]string, 0, len(namespaces))
		for _, namespace := range namespaces {
			items = append(items, namespace.Name)
		}
		return items, nil
	})
}

func filterCompletions(prefix string, load func() ([]string, error)) ([]string, error) {
	items, err := load()
	if err != nil {
		return nil, err
	}

	filtered := make([]string, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		if strings.HasPrefix(item, prefix) {
			filtered = append(filtered, item)
		}
	}
	return filtered, nil
}

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func parseSecretRefs(items []string) ([]models.SecretRef, error) {
	result := make([]models.SecretRef, 0, len(items))
	for _, item := range items {
		parts := strings.Split(item, ":")
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid --secret value %q, expected name:key:ENV_VAR", item)
		}
		result = append(result, models.SecretRef{
			Name:   strings.TrimSpace(parts[0]),
			Key:    strings.TrimSpace(parts[1]),
			EnvVar: strings.TrimSpace(parts[2]),
		})
	}
	return result, nil
}

func parseVolumeMounts(items []string) ([]models.VolumeMount, error) {
	result := make([]models.VolumeMount, 0, len(items))
	for _, item := range items {
		parts := strings.Split(item, ":")
		if len(parts) < 2 || len(parts) > 3 {
			return nil, fmt.Errorf("invalid --volume value %q, expected source:target[:ro]", item)
		}
		mount := models.VolumeMount{
			Source: strings.TrimSpace(parts[0]),
			Target: strings.TrimSpace(parts[1]),
		}
		if len(parts) == 3 {
			if strings.TrimSpace(parts[2]) != "ro" {
				return nil, fmt.Errorf("invalid --volume mode %q, only ro is supported", parts[2])
			}
			mount.ReadOnly = true
		}
		result = append(result, mount)
	}
	return result, nil
}

// visibleLen calculates the length of a string without ANSI escape codes.
func visibleLen(s string) int {
	re := regexp.MustCompile(`\x1b\[[0-9;]*[a-zA-Z]`)
	return len(re.ReplaceAllString(s, ""))
}

// padRight pads a string with spaces to the right, accounting for hidden ANSI codes.
func padRight(s string, width int) string {
	vLen := visibleLen(s)
	if vLen >= width {
		return s
	}
	return s + strings.Repeat(" ", width-vLen)
}
