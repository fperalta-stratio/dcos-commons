package com.mesosphere.sdk.scheduler;

import com.mesosphere.sdk.dcos.Capabilities;
import com.mesosphere.sdk.offer.Constants;
import com.mesosphere.sdk.offer.evaluate.placement.*;
import com.mesosphere.sdk.scheduler.plan.DefaultPlan;
import com.mesosphere.sdk.scheduler.plan.Phase;
import com.mesosphere.sdk.scheduler.plan.Plan;
import com.mesosphere.sdk.specification.DefaultPodSpec;
import com.mesosphere.sdk.specification.DefaultServiceSpec;
import com.mesosphere.sdk.specification.PodSpec;
import com.mesosphere.sdk.specification.ServiceSpec;
import com.mesosphere.sdk.storage.MemPersister;
import com.mesosphere.sdk.storage.Persister;
import com.mesosphere.sdk.testutils.SchedulerConfigTestUtils;
import com.mesosphere.sdk.testutils.TestConstants;
import com.mesosphere.sdk.testutils.TestPodFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

/**
 * This class tests the {@link SchedulerBuilder}.
 */
public class SchedulerBuilderTest {

    private static final SchedulerConfig SCHEDULER_CONFIG = SchedulerConfigTestUtils.getTestSchedulerConfig();

    @Mock private Capabilities mockCapabilities;

    @Before
    public void beforeEach() {
        MockitoAnnotations.initMocks(this);
        when(mockCapabilities.supportsDomains()).thenReturn(true);
        Capabilities.overrideCapabilities(mockCapabilities);
    }

    @Test
    public void leaveRegionRuleUnmodified() {
        PodSpec originalPodSpec = DefaultPodSpec.newBuilder(getPodSpec())
                .placementRule(getRemoteRegionRule())
                .build();
        PodSpec updatedPodSpec = SchedulerBuilder.updatePodPlacement(originalPodSpec);

        Assert.assertEquals(originalPodSpec, updatedPodSpec);
    }

    @Test
    public void setLocalRegionRule() {
        PodSpec originalPodSpec = getPodSpec();
        PodSpec updatedPodSpec = SchedulerBuilder.updatePodPlacement(originalPodSpec);

        Assert.assertTrue(updatedPodSpec.getPlacementRule().isPresent());
        Assert.assertTrue(updatedPodSpec.getPlacementRule().get() instanceof IsLocalRegionRule);
    }

    @Test
    public void addLocalRegionRule() {
        PodSpec originalPodSpec = DefaultPodSpec.newBuilder(getPodSpec())
                .placementRule(ZoneRuleFactory.getInstance().require(ExactMatcher.create(TestConstants.ZONE)))
                .build();
        PodSpec updatedPodSpec = SchedulerBuilder.updatePodPlacement(originalPodSpec);

        Assert.assertTrue(updatedPodSpec.getPlacementRule().isPresent());
        Assert.assertTrue(updatedPodSpec.getPlacementRule().get() instanceof AndRule);
        Assert.assertTrue(PlacementUtils.placementRuleReferencesRegion(updatedPodSpec));
    }

    @Test
    public void testDeployPlanOverriddenDuringUpdate() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("valid-minimal.yml").getFile());
        DefaultServiceSpec serviceSpec = DefaultServiceSpec.newGenerator(file, SCHEDULER_CONFIG).build();
        Persister persister = new MemPersister();
        SchedulerBuilder builder = new SchedulerBuilder(serviceSpec, SCHEDULER_CONFIG, persister);

        Collection<Plan> plans = builder.selectDeployPlan(getDeployUpdatePlans(), true);

        Assert.assertEquals(1, plans.size());
        Plan deployPlan = plans.stream()
                .filter(plan -> plan.isDeployPlan())
                .findFirst().get();

        Assert.assertEquals(1, deployPlan.getChildren().size());
    }

    @Test
    public void testDeployPlanPreservedDuringInstall() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("valid-minimal.yml").getFile());
        DefaultServiceSpec serviceSpec = DefaultServiceSpec.newGenerator(file, SCHEDULER_CONFIG).build();
        Persister persister = new MemPersister();
        SchedulerBuilder builder = new SchedulerBuilder(serviceSpec, SCHEDULER_CONFIG, persister);

        Collection<Plan> plans = builder.selectDeployPlan(getDeployUpdatePlans(), false);

        Assert.assertEquals(2, plans.size());
        Plan deployPlan = plans.stream()
                .filter(plan -> plan.isDeployPlan())
                .findFirst().get();

        Assert.assertEquals(2, deployPlan.getChildren().size());
    }

    @Test
    public void doNotConstrainToRegionWhenNotSupported() throws Exception {
        Capabilities capabilities = mock(Capabilities.class);
        when(capabilities.supportsDomains()).thenReturn(false);
        Capabilities.overrideCapabilities(capabilities);

        ServiceSpec serviceSpec = DefaultServiceSpec.newBuilder()
                .name(TestConstants.SERVICE_NAME)
                .role(TestConstants.ROLE)
                .principal(TestConstants.PRINCIPAL)
                .zookeeperConnection("badhost-shouldbeignored:2181")
                .pods(Arrays.asList(getPodSpec()))
                .user(TestConstants.SERVICE_USER)
                .build();

        SchedulerBuilder builder = DefaultScheduler.newBuilder(
                serviceSpec, SchedulerConfigTestUtils.getTestSchedulerConfig(), new MemPersister());
        Optional<PlacementRule> placementRule = builder.getServiceSpec().getPods().get(0).getPlacementRule();

        Assert.assertFalse(placementRule.isPresent());
    }

    @Test
    public void constrainToSingleRegion() {
        PlacementRule placementRule = SchedulerBuilder.getRegionRule(Optional.empty());
        Assert.assertTrue(placementRule instanceof IsLocalRegionRule);

        placementRule = SchedulerBuilder.getRegionRule(Optional.of("USA"));
        Assert.assertTrue(placementRule instanceof RegionRule);
    }

    private PlacementRule getRemoteRegionRule() {
        return RegionRuleFactory.getInstance().require(ExactMatcher.create(TestConstants.REMOTE_REGION));
    }

    private static PodSpec getPodSpec() {
        return TestPodFactory.getPodSpec(
                TestConstants.POD_TYPE,
                TestConstants.RESOURCE_SET_ID,
                TestConstants.TASK_NAME,
                TestConstants.TASK_CMD,
                TestConstants.SERVICE_USER,
                1,
                1.0,
                256,
                4096);
    }

    // Deploy plan has 2 phases, update plan has 1 for distinguishing which was chosen.
    private static Collection<Plan> getDeployUpdatePlans() {
        Phase phase = mock(Phase.class);

        Plan deployPlan = new DefaultPlan(Constants.DEPLOY_PLAN_NAME, Arrays.asList(phase, phase));
        Assert.assertEquals(2, deployPlan.getChildren().size());

        Plan updatePlan = new DefaultPlan(Constants.UPDATE_PLAN_NAME, Arrays.asList(phase));
        Assert.assertEquals(1, updatePlan.getChildren().size());

        return Arrays.asList(deployPlan, updatePlan);
    }
}
